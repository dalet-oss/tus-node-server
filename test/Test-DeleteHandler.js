/* eslint-env node, mocha */
'use strict';

const assert = require('assert');
const http = require('http');
const should = require('should');
const DeleteHandler = require('../lib/handlers/DeleteHandler');
const DataStore = require('../lib/stores/FileStore');

const hasHeader = (res, header) => {
    const key = Object.keys(header)[0];
    return res._header.indexOf(`${key}: ${header[key]}`) > -1;
};

describe('DeleteHandler', () => {
    const path = '/files';
    let res = null;
    const namingFunction = (req) => req.url.replace(/\//g, '-');
    const store = new DataStore({ path, namingFunction });
    const handler = new DeleteHandler(store);
    const req = { headers: {}, url: '/files' };

    beforeEach((done) => {
        res = new http.ServerResponse({ method: 'DELETE' });
        done();
    });

    describe('remove()', () => {
        it("must 404 if the file doesn't exist", (done) => {
            req.url = `${path}/1234`;
            handler.send(req, res).then(() => {
                assert.equal(res.statusCode, 404);
                return done();
            })
                .catch(done);
        });

    });

});
