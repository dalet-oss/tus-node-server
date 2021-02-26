'use strict';

const assert = require('assert');
const File = require('../models/File');
const DataStore = require('./DataStore');
const aws = require('aws-sdk');
const ERRORS = require('../constants').ERRORS;
const EVENTS = require('../constants').EVENTS;
const TUS_RESUMABLE = require('../constants').TUS_RESUMABLE;
const crypt = require('crypto');

const debug = require('debug');
const log = debug('tus-node-server:stores:s3store');

const MAX_PARTS_COUNT = 10000;
const OPTIMAL_PART_SIZE = 10 * 1024 * 1024;

/**
 * TODO
 * - support smaller file sizes
 *     - if file is smaller than min_part_size, upload with `putObject`
 * - add support for `chunkSize: Infinity` (stream splitter?)
 * - improve error handling
 * - tests :)
 * - support other extensions like termination
 */

// Implementation (based on https://github.com/tus/tusd/blob/master/s3store/s3store.go)
//
// Once a new tus upload is initiated, multiple objects in S3 are created:
//
// First of all, a new info object is stored which contains (as Metadata) a JSON-encoded
// blob of general information about the upload including its size and meta data.
// This kind of objects have the suffix ".info" in their key.
//
// In addition a new multipart upload
// (http://docs.aws.amazon.com/AmazonS3/latest/dev/uploadobjusingmpu.html) is
// created. Whenever a new chunk is uploaded to tus-node-server using a PATCH request, a
// new part is pushed to the multipart upload on S3.
//
// If meta data is associated with the upload during creation, it will be added
// to the multipart upload and after finishing it, the meta data will be passed
// to the final object. However, the metadata which will be attached to the
// final object can only contain ASCII characters and every non-ASCII character
// will be replaced by a question mark (for example, "Menü" will be "Men?").
// However, this does not apply for the metadata returned by the `_getMetadata`
// function since it relies on the info object for reading the metadata.
// Therefore, HEAD responses will always contain the unchanged metadata, Base64-
// encoded, even if it contains non-ASCII characters.
//
// Once the upload is finished, the multipart upload is completed, resulting in
// the entire file being stored in the bucket. The info object, containing
// meta data is not deleted.
//
// Considerations
//
// In order to support tus' principle of resumable upload, S3's Multipart-Uploads
// are internally used.
// For each incoming PATCH request (a call to `write`), a new part is uploaded
// to S3.

class S3Store extends DataStore {
    constructor(options) {
        super(options);

        this.extensions = ['creation', 'creation-defer-length'];

        assert.ok(options.accessKeyId, '[S3Store] `accessKeyId` must be set');
        assert.ok(options.secretAccessKey, '[S3Store] `secretAccessKey` must be set');
        assert.ok(options.region, '[S3Store] `region` must be set');
        assert.ok(options.bucket, '[S3Store] `bucket` must be set');

        this.tmp_dir_prefix = options.tmpDirPrefix || 'tus-s3-store';
        this.bucket_name = options.bucket;
        this.part_size = options.partSize || 8 * 1024 * 1024;

        // cache object to save upload data
        // avoiding multiple http calls to s3
        this.cache = {};

        delete options.partSize;

        this.client = new aws.S3(Object.assign({}, {
            apiVersion: '2006-03-01',
        }, options));

        log('init');
    }

    /**
     * Check if the bucket exists in S3.
     *
     * @return {Promise}
     */
    _bucketExists() {
        return this.client.headBucket({ Bucket: this.bucket_name })
            .promise()
            .then((data) => {
                if (!data) {
                    throw new Error(`bucket "${this.bucket_name}" does not exist`);
                }

                log(`bucket "${this.bucket_name}" exists`);

                return data;
            })
            .catch((err) => {
                if (err.statusCode === 404) {
                    throw new Error(`[S3Store] bucket "${this.bucket_name}" does not exist`);
                }
                else {
                    throw new Error(err);
                }
            });
    }

    /**
     * Creates a multipart upload on S3 attaching any metadata to it.
     * Also, a `${file_id}.info` file is created which holds some information
     * about the upload itself like: `upload_id`, `upload_length`, etc.
     *
     * @param  {Object}          file file instance
     * @return {Promise<Object>}      upload data
     */
    _initMultipartUpload(file, parsedMetadata) {
        log(`[${file.id}] initializing multipart upload`);

        const upload_data = {
            Bucket: this.bucket_name,
            Key: file.id,
            Metadata: {
                upload_length: file.upload_length,
                tus_version: TUS_RESUMABLE,
                upload_metadata: file.upload_metadata,
                // upload_defer_length: upload_defer_length,
            },
        };

        if (parsedMetadata.contentType) {
            upload_data.ContentType = parsedMetadata.contentType.decoded;
        }

        if (parsedMetadata.filename) {
            upload_data.Metadata.original_name = parsedMetadata.filename.encoded;
        }

        return this.client
            .createMultipartUpload(upload_data)
            .promise()
            .then((data) => {
                log(`[${file.id}] multipart upload created (${data.UploadId})`);

                return data.UploadId;
            })
            .then((upload_id) => this._saveMetadata(file, upload_id))
            .catch((err) => {
                throw err;
            });
    }

    /**
     * Saves upload metadata to a `${file_id}.info` file on S3.
     * Please note that the file is empty and the metadata is saved
     * on the S3 object's `Metadata` field, so that only a `headObject`
     * is necessary to retrieve the data.
     *
     * @param  {Object}          file      file instance
     * @param  {String}          upload_id S3 upload id
     * @return {Promise<Object>}           upload data
     */
    _saveMetadata(file, upload_id) {
        log(`[${file.id}] saving metadata`);

        const metadata = {
            file: JSON.stringify(file),
            upload_id,
            tus_version: TUS_RESUMABLE,
        };

        return this.client
            .putObject({
                Bucket: this.bucket_name,
                Key: `${file.id}.info`,
                Body: '',
                Metadata: metadata,
            })
            .promise()
            .then(() => {
                log(`[${file.id}] metadata file saved`);

                return {
                    file,
                    upload_id,
                };
            })
            .catch((err) => {
                throw err;
            });
    }

    /**
     * Retrieves upload metadata previously saved in `${file_id}.info`.
     * There's a small and simple caching mechanism to avoid multiple
     * HTTP calls to S3.
     *
     * @param  {String} file_id id of the file
     * @return {Promise<Object>}        which resolves with the metadata
     */
    getMetadata(file_id) {
        log(`[${file_id}] retrieving metadata`);

        if (this.cache[file_id] && this.cache[file_id].file) {
            log(`[${file_id}] metadata from cache`);

            return Promise.resolve(this.cache[file_id]);
        }

        log(`[${file_id}] metadata from s3`);

        return this.client
            .headObject({
                Bucket: this.bucket_name,
                Key: `${file_id}.info`,
            })
            .promise()
            .then((data) => {
                this.cache[file_id] = Object.assign({}, data.Metadata, {
                    file: JSON.parse(data.Metadata.file),
                });

                return this.cache[file_id];
            })
            .catch((err) => {
                throw err;
            });
    }

    /**
     * Parses the Base64 encoded metadata received from the client.
     *
     * @param  {String} metadata_string tus' standard upload metadata
     * @return {Object}                 metadata as key-value pair
     */
    _parseMetadataString(metadata_string) {
        const kv_pair_list = metadata_string.split(',');

        return kv_pair_list.reduce((metadata, kv_pair) => {
            const [key, base64_value] = kv_pair.split(' ');

            metadata[key] = {
                encoded: base64_value,
                decoded: Buffer.from(base64_value, 'base64').toString('ascii'),
            };

            return metadata;
        }, {});
    }


    /**
     * Returns MD5 hash for buffer.
     *
     * @param  {Buffer}          buffer            buffer
     * @return {String}                     MD5 hash
     */
    _getMD5HashFromBuffer(buffer) {
        return crypt.createHash('md5')
            .update(buffer)
            .digest('base64');
    }


    /**
     * Uploads a part/chunk to S3 from a temporary part file.
     *
     * @param  {Object}          metadata            upload metadata
     * @param  {Stream}          read_stream         incoming request read stream
     * @param  {Number}          current_part_number number of the current part/chunk
     * @param {Number}           initial_offset      number of uploaded bites
     * @return {Promise<String>}                     which resolves with the parts' etag
     */
    _uploadPart(metadata, read_stream, current_part_number, initial_offset) {

        return new Promise((resolve, reject) => {
            const progressEmitter = this._getUploadingProgressEmitter();
            const chunkSize = this._calculateUploadPartSize(metadata.file.upload_length);
            let uploadedLength = initial_offset;
            read_stream.on('error', async(err) => {
                progressEmitter(metadata.file, uploadedLength, true);
                await progressEmitter.flush();
                reject(err);
            });

            let chunkAccumulator = null;
            read_stream.on('data', (chunk) => {
                // it reads in chunks of 64KB. We accumulate them up to 10MB and then we send to S3
                if (chunkAccumulator === null) {
                    chunkAccumulator = chunk;
                }
                else {
                    chunkAccumulator = Buffer.concat([chunkAccumulator, chunk]);
                }
                log(`[${metadata.file.id}] Accumulating chunk for part #${current_part_number}`);
                log(`[${metadata.file.id}] total size ${uploadedLength}`);
                if (chunkAccumulator.length > chunkSize) {
                    // pause the stream to upload this chunk to S3
                    read_stream.pause();
                    log(`[${metadata.file.id}] uploading part #${current_part_number}(${chunkAccumulator.length}) to s3`);
                    this.client.uploadPart({
                        Bucket: this.bucket_name,
                        Key: metadata.file.id,
                        UploadId: metadata.upload_id,
                        PartNumber: current_part_number,
                        Body: chunkAccumulator,
                        ContentLength: chunkAccumulator.length,
                        ContentMD5: this._getMD5HashFromBuffer(chunkAccumulator),
                    }).promise()
                        .then((data) => {
                            // resume to read the next chunk
                            read_stream.resume();
                            log(`[${metadata.file.id}] finished uploading part #${current_part_number}`);
                            current_part_number++;
                            uploadedLength += chunkAccumulator.length;
                            chunkAccumulator = null;
                            return progressEmitter(metadata.file, uploadedLength);
                        }).catch((err) => {
                            console.error(`error uploading the chunk to S3 ${err.message}`);
                            reject(err);
                        });
                }
            });

            read_stream.on('close', async() => {
                log(`[${metadata.file.id}] Connection closed`);
                if (metadata.file && parseInt(metadata.file.upload_length, 10) > uploadedLength) {
                    progressEmitter(metadata.file, uploadedLength, true);
                    await progressEmitter.flush();
                    this.emit(EVENTS.EVENT_UPLOAD_SUSPENDED, { file: metadata.file });
                }
            });

            read_stream.on('end', () => {
                if (chunkAccumulator) {
                    log(`[${metadata.file.id}] uploading last part #${current_part_number}(${chunkAccumulator.length}) to s3`);
                    return this.client.uploadPart({
                        Bucket: this.bucket_name,
                        Key: metadata.file.id,
                        UploadId: metadata.upload_id,
                        PartNumber: current_part_number,
                        Body: chunkAccumulator,
                        ContentLength: chunkAccumulator.length,
                        ContentMD5: this._getMD5HashFromBuffer(chunkAccumulator),
                    }).promise()
                        .then(async(data) => {
                            // resume to read the next chunk
                            read_stream.resume();
                            log(`[${metadata.file.id}] finished uploading part #${current_part_number}`);
                            current_part_number++;
                            uploadedLength += chunkAccumulator.length;
                            chunkAccumulator = null;
                            log(`[${metadata.file.id}] Emitting last progress event`);
                            progressEmitter(metadata.file, uploadedLength, true);
                            await progressEmitter.flush();
                            log(`[${metadata.file.id}] Last progress event successfully emitted`);
                            return resolve();
                        }).catch((err) => {
                            console.error(`error uploading the chunk to S3 ${err.message}`);
                            reject(err);
                        });
                }
                return resolve();
            });
        });
    }

    /**
     * Calculating real upload part size:
     *
     * @param {Number}                totalLength         length of the uploading file
     * @return {Number}                                   minimum required part size
     */
    _calculateUploadPartSize(totalLength) {
        if (totalLength < OPTIMAL_PART_SIZE) {
            return totalLength;
        }
        const realPartSize = Math.ceil(totalLength / MAX_PARTS_COUNT);
        if (realPartSize < OPTIMAL_PART_SIZE) {
            return OPTIMAL_PART_SIZE;
        }
        return realPartSize;
    }


    /**
     * Processes the following steps:
     * - uploads a part (stream) to s3
     * - finishes the upload in case offset === length
     *
     * @param {Object}                metadata            upload metadata
     * @param {http<IncomingMessage>} req                 incoming request
     * @param {Number}                current_part_number number of the current part/chunk
     * @param {Number}                initial_offset      number of uploaded bites
     * @return {Promise<Number>}                          which resolves with the current offset
     * @memberof S3Store
     */
    _processUpload(metadata, req, current_part_number, initial_offset) {
        return this._uploadPart(metadata, req, current_part_number, initial_offset)
            .then(() => this.getOffset(metadata.file.id, true))
            .then((current_offset) => {
                log(`[${metadata.file.id}] current offset: ${current_offset.size}`);
                if (parseInt(metadata.file.upload_length, 10) === current_offset.size) {
                    return this._finishMultipartUpload(metadata, current_offset.parts)
                        .then(async(location) => {
                            log(`[${metadata.file.id}] finished uploading: ${location}`);

                            await this.emit(EVENTS.EVENT_UPLOAD_COMPLETE, {
                                file: Object.assign({}, metadata.file, { location }),
                            });

                            this._clearCache(metadata.file.id);

                            return current_offset.size;
                        })
                        .catch((err) => {
                            throw err;
                        });
                }

                return current_offset.size;
            });
    }

    /**
     * Completes a multipart upload on S3.
     * This is where S3 concatenates all the uploaded parts.
     *
     * @param  {Object}          metadata upload metadata
     * @param  {Array}           parts    data of each part
     * @return {Promise<String>}          which resolves with the file location on S3
     */
    _finishMultipartUpload(metadata, parts) {
        return this.client
            .completeMultipartUpload({
                Bucket: this.bucket_name,
                Key: metadata.file.id,
                UploadId: metadata.upload_id,
                MultipartUpload: {
                    Parts: parts.map((part) => {
                        return {
                            ETag: part.ETag,
                            PartNumber: part.PartNumber,
                        };
                    }),
                },
            })
            .promise()
            .then((result) => result.Location)
            .catch((err) => {
                throw err;
            });
    }

    /**
     * Gets the number of parts/chunks
     * already uploaded to S3.
     *
     * @param  {String}          file_id            id of the file
     * @param  {String}          part_number_marker optional part number marker
     * @return {Promise<Array>}                    upload parts
     */
    _retrieveParts(file_id, part_number_marker,) {
        const params = {
            Bucket: this.bucket_name,
            Key: file_id,
            UploadId: this.cache[file_id].upload_id,
        };

        if (part_number_marker) {
            params.PartNumberMarker = part_number_marker;
        }
        // TODO only for debug! Remove after debugging
        const prevNextPartNumberMarker = part_number_marker ? parseInt(part_number_marker, 10) : undefined;

        return this.client
            .listParts(params)
            .promise()
            .then((data) => {
                if (process.env.USED_LOCALSTACK && data.NextPartNumberMarker) {
                    data.NextPartNumberMarker++;
                }
                if (data.NextPartNumberMarker && (!prevNextPartNumberMarker || prevNextPartNumberMarker !== data.NextPartNumberMarker)) {
                    // if (data.NextPartNumberMarker) {
                    return this._retrieveParts(file_id, data.NextPartNumberMarker)
                        .then((val) => {
                            const uniqueParts = val.filter((e) => !data.Parts.find((part) => part.ETag === e.ETag));
                            return [].concat(data.Parts, uniqueParts);
                        });
                }

                return data.Parts;
            });
    }

    /**
     * Gets the number of parts/chunks
     * already uploaded to S3.
     *
     * @param  {String}          file_id            id of the file
     * @return {Promise<Number>}                    number of parts
     */
    _countParts(file_id) {
        return this._retrieveParts(file_id)
            .then((parts) => parts.length);
    }

    /**
     * Removes cached data for a given file.
     * @param  {String} file_id id of the file
     * @return {undefined}
     */
    _clearCache(file_id) {
        log(`[${file_id}] removing cached data`);
        delete this.cache[file_id];
    }

    create(req) {
        const upload_length = req.headers['upload-length'];
        const upload_defer_length = req.headers['upload-defer-length'];
        const upload_metadata = req.headers['upload-metadata'];

        if (upload_length === undefined && upload_defer_length === undefined) {
            throw new Error(ERRORS.INVALID_LENGTH);
        }

        let file_id;
        // TODO need to fix
        const parsedMetadata = this._parseMetadataString(upload_metadata);
        try {
            let file_extension;
            if (parsedMetadata.file_extension) {
                file_extension = parsedMetadata.file_extension.decoded;
            }
            file_id = file_extension ? `${this.generateFileName(req)}.${file_extension}` : this.generateFileName(req);
        }
        catch (err) {
            console.warn('[S3Store] create: check your `namingFunction`. Error', err);
            throw new Error(ERRORS.FILE_WRITE_ERROR);
        }

        const file = new File(file_id, upload_length, upload_defer_length, upload_metadata);

        return this._bucketExists()
            .then(() => this._initMultipartUpload(file, parsedMetadata))
            .then(async(data) => {
                await this.emit(EVENTS.EVENT_FILE_CREATED, data);

                return data.file;
            })
            .catch((err) => {
                this._clearCache(file_id);
                throw err;
            });
    }

    write(req, file_id, offset) {
        return new Promise((resolve, reject) => {
            Promise
                .all([
                    this._countParts(file_id),
                    this.getMetadata(file_id),
                ])
                .then((results) => {
                    const [part_number, metadata] = results;
                    const current_part_number = part_number + 1;
                    const total_parts = Math.ceil(
                        parseInt(metadata.file.upload_length, 10) / this.part_size
                    );

                    // s3 sdk requires a content length
                    // even though it is a stream (?)
                    if (current_part_number === total_parts) {
                        req.length = metadata.file.upload_length - offset;
                    }
                    else {
                        req.length = this.part_size;
                    }

                    // we need to know if the connection was closed
                    // to gracefully handle s3 sdk errors
                    let isClosed = false;
                    req.on('close', () => {
                        isClosed = true;
                    });

                    return this._processUpload(metadata, req, current_part_number, offset)
                        .then(resolve)
                        .catch((err) => {
                            // we'll get in here when the client closes/cancels
                            // the request however s3 is expecting more data since
                            // we had to set a content length in the incoming stream
                            if (['RequestTimeout', 'NoSuchUpload'].includes(err.code) && isClosed) {
                                if (err.code === 'RequestTimeout') {
                                    console.warn('[S3Store] Request "close" event was emitted,'
                                        + 'however S3 was expecting more data. Failing gracefully...');
                                }

                                if (err.code === 'NoSuchUpload') {
                                    console.warn('[S3Store] Request "close" event was emitted,'
                                        + 'however S3 was expecting more data. Most likely the'
                                        + ' upload is already finished/aborted. Failing gracefully...');
                                }

                                return this.getOffset(file_id, true)
                                    .then((current_offset) => resolve(current_offset.size))
                                    .catch(reject);
                            }

                            this._clearCache(file_id);

                            console.error(err);
                            return reject(err);
                        });
                })
                .catch(reject);
        });
    }

    getOffset(file_id, with_parts = false) {
        return new Promise((resolve, reject) => {
            this.getMetadata(file_id)
                .then((metadata) => {
                    return this._retrieveParts(file_id)
                        .then((parts) => {
                            return {
                                parts,
                                metadata,
                            };
                        })
                        .catch((err) => {
                            throw err;
                        });
                })
                .then((data) => {
                    // if no parts are found, offset is 0
                    if (data.parts.length === 0) {
                        return resolve({
                            size: 0,
                            upload_length: data.metadata.file.upload_length,
                        });
                    }

                    const offset = data.parts.reduce((a, b) => {
                        a += parseInt(b.Size, 10);

                        return a;
                    }, 0);

                    let output = Object.assign({}, this.cache[file_id].file, {
                        size: offset,
                    });

                    if (with_parts) {
                        output = Object.assign(output, { parts: data.parts });
                    }

                    return resolve(output);
                })
                .catch((err) => {
                    if (['NotFound', 'NoSuchUpload'].includes(err.code)) {
                        console.error(err);
                        console.warn('[S3Store] getOffset: No file found.');

                        return reject(ERRORS.FILE_NOT_FOUND);
                    }

                    throw err;
                });
        });
    }


    /*
    * TODO pay attention to the abortMultipartUpload API description:
    * This operation aborts a multipart upload. After a multipart upload is aborted, no additional parts can be
    * uploaded using that upload ID. The storage consumed by any previously uploaded parts will be freed. However,
    * if any part uploads are currently in progress, those part uploads might or might not succeed. As a result,
    * it might be necessary to abort a given multipart upload multiple times in order to completely free all storage
    * consumed by all parts.
    * To verify that all parts have been removed, so you don't get charged for the part storage, you should call the
    * ListParts operation and ensure that the parts list is empty.
    * */
    /**
     * Removing multipleUpload, metadata and content files.
     *
     * @param  {object} req http.incomingMessage
     * @param  {string} file_id name of the file
     * @return {Promise}
     */
    remove(req, file_id) {
        return new Promise((resolve, reject) => {
            this.getMetadata(file_id).then((metadata) => {
                const upload_data = {
                    Bucket: this.bucket_name,
                    Key: file_id,
                    UploadId: metadata.upload_id,
                };
                return this.client.abortMultipartUpload(upload_data).promise().then(() => {
                    this._clearCache(file_id);
                    return this._removeObjectsFromS3(file_id)
                        .then(() => {
                            this.emit(EVENTS.EVENT_FILE_REMOVED, { file: metadata.file });
                            return resolve();
                        })
                        .catch((err) => {
                            reject(err);
                        });
                }).catch((err) => {
                    reject(err);
                });
            }).catch((err) => {
                if (err.code === 'NotFound') {
                    reject(ERRORS.FILE_NOT_FOUND);
                }
            });
        });
    }

    /**
     * Removing objects from S3.
     *
     * @param  {String} file_id     name of the file
     * @return {Promise}
     */
    _removeObjectsFromS3(file_id) {
        const objectsToDelete = {
            Bucket: this.bucket_name,
            Delete: {
                Objects: [
                    {
                        Key: file_id,
                    },
                    {
                        Key: `${file_id}.info`,
                    },
                ],
            },
        };
        return this.client.deleteObjects(objectsToDelete).promise();
    }
}

module.exports = S3Store;
