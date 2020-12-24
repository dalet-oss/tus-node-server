'use strict';

const BaseHandler = require('./BaseHandler');
const ERRORS = require('../constants').ERRORS;
const debug = require('debug');
const log = debug('tus-node-server:handlers:delete');
class DeleteHandler extends BaseHandler {
    /**
     * Delete a file in the DataStore.
     *
     * @param  {object} req http.incomingMessage
     * @param  {object} res http.ServerResponse
     * @return {function}
     */
    send(req, res) {
        const file_id = this.getFileIdFromRequest(req);
        return this.store.remove(req, file_id).then(() => {
            return super.send(res, 204);
        })
            .catch((error) => {
                log('[DeleteHandler]', error);
                const status_code = error.status_code || ERRORS.FILE_REMOVE_ERROR.status_code;
                const body = error.body || `${ERRORS.FILE_REMOVE_ERROR.body}${error.message || ''}\n`;
                return super.send(res, status_code, {}, body);
            });
    }
}

module.exports = DeleteHandler;
