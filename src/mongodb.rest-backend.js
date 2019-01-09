/**
 * The MIT License (MIT)
 * 
 * Copyright (c) 2019 Covistra Technologies Inc.
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation 
 * files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, 
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished 
 * to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES 
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE 
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN 
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
const { MongoClient } = require('mongodb');
const get = require('lodash.get');

module.exports = function({ logger, context }) {

    logger.debug("Initializing the MongoDB REST Backend", context.get('mongodb'));

    let cfg = context.get('mongodb');
    let client = MongoClient.connect(cfg.url, { useNewUrlParser: true });

    function getIdField(operation) {
        return get(operation, "resource.backendConfig.idField") || "_id";
    }

    function getCollection(operation) {
        let collectionName = get(operation, "resource.backendConfig.collection");
        return client.then(conn => conn.db(cfg.name)).then(db => db.collection(collectionName));
    }

    class MongoDBRestBackend {
        list(operation) {
            logger.debug("mongodb:list", operation);
            return getCollection(operation).then(coll => coll.find({}).toArray());
        }
        read(operation) {
            let q = {};
            let idField = getIdField(operation);
            if (Array.isArray(idField)) {
                q.$or = idField.map(idf => {
                    let q1 = {};
                    q1[idf] = operation.id
                    return q1;
                });
            } else {
                q[idField] = operation.id;
            }
            logger.debug("mongo:read:", q);
            return getCollection(operation).then(coll => coll.findOne(q));
        }
        create(operation) {
            return operation.payload.then(data => getCollection(operation).then(coll => coll.insertOne(data)));
        }
        update(operation) {
            let q = {};
            let idField = getIdField(operation);
            if (Array.isArray(idField)) {
                q.$or = idField.map(idf => {
                    let q1 = {};
                    q1[idf] = operation.id
                    return q1;
                });
            } else {
                q[idField] = operation.id;
            }
            logger.debug("mongo:update:", q);
            return operation.payload.then(data => getCollection(operation).then(coll => coll.updateOne(q, data)));
        }
        patch(operation) {
            let q = {};
            let idField = getIdField(operation);
            if (Array.isArray(idField)) {
                q.$or = idField.map(idf => {
                    let q1 = {};
                    q1[idf] = operation.id
                    return q1;
                });
            } else {
                q[idField] = operation.id;
            }
            logger.debug("mongo:patch:", q);
            return operation.payload.then(data => getCollection(operation).then(coll => {

                let updateOp = {};
                if (data.$set || data.$unset || data.$push || data.$pull) {
                    if (data.$set) {
                        updateOp.$set = data.$set;
                    }
                    if (data.$unset) {
                        updateOp.$unset = data.$unset;
                    }
                    if (data.$push) {
                        updateOp.$push = data.$push;
                    }
                    if (data.$pull) {
                        updateOp.$push = data.$pull;
                    }
                } else {
                    updateOp = { $set: data };
                }

                return coll.updateOne(q, updateOp);
            }));
        }
        remove(operation) {
            let q = {};
            let idField = getIdField(operation);
            if (Array.isArray(idField)) {
                q.$or = idField.map(idf => {
                    let q1 = {};
                    q1[idf] = operation.id
                    return q1;
                });
            } else {
                q[idField] = operation.id;
            }
            logger.debug("mongo:remove:", q);
            return getCollection(operation).then(coll => coll.removeOne(q));
        }
    }

    return new MongoDBRestBackend();
}