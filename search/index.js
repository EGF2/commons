"use strict";

const elasticsearch = require("elasticsearch");

let dateRegExp = /^\d{4}-\d{2}-\d{2}$/;

/* eslint camelcase: 0 */

class Searcher {
    constructor(config) {
        this.elastic = new elasticsearch.Client(config);
    }

    createRequest(options) {
        let filters = [];
        let notFilters = [];
        let filedExists;
        Object.keys(options.filters || {}).forEach(key => {
            let val = options.filters[key];
            if (val.length === 1) {
                val = val[0];
            }

            if (val === "exists") {
                filters.push({ exists: { field: key } });
            } else if (dateRegExp.test(val)) {
                filters.push({
                    range: {
                        [key]: {
                            gte: val,
                            lt: val + "||+1d"
                        }
                    }
                });
            } else {
                if (!Array.isArray(val)) {
                    val = [val];
                }

                let eqVals = val.filter(val => !val.startsWith("!"));
                if (eqVals && eqVals.length) {
                    filters.push({ terms: { [key]: eqVals } });
                }

                let notEqVals;
                if (val[0] === "!") {
                    filedExists = key;
                }
                notEqVals = val.filter(val => val.startsWith("!")).map(val => val.slice(1));
                if (notEqVals && notEqVals.length) {
                    notFilters.push({ terms: { [key]: notEqVals } });
                }
            }
        });

        let range = {};
        Object.keys(options.range || {}).forEach(key => {
            range[key] = {};
            if (options.range[key].gte) {
                range[key].gte = options.range[key].gte;
            }
            if (options.range[key].lte) {
                range[key].lte = options.range[key].lte;
            }
        });

        if (Object.keys(range).length) {
            filters.push({
                range: range
            });
        }

        let query = { query: {} };

        let isEmail = /^(?:[a-z0-9!#$%&amp;'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&amp;'*+/=?^_`{|}~-]+)*|"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])$/;

        if (options.fields && options.q) {
            let reg = /[^\w\s\.]/gi;
            let checkNumberEmail = false;
            if (isEmail.test(options.q)) {
                let emailArr = options.q.split(".");
                let lastSymbol = emailArr[emailArr.length - 2];
                checkNumberEmail = Number.isInteger(parseInt(lastSymbol[lastSymbol.length - 1]));
            }

            if (checkNumberEmail) {
                reg = /[^\w\s]/gi;
            }
            query.query.bool = query.query.bool || {};
            options.q = options.q.replace(reg, " AND ");
            query.query.bool.must = {
                query_string: {
                    query: `*${options.q}*`,
                    fields: options.fields
                }
            };
        }

        if (filters.length || notFilters.length || filedExists) {
            query.query.bool = query.query.bool || {};
            if (filters.length) {
                query.query.bool.filter = filters;
            }
            if (notFilters.length) {
                query.query.bool.must_not = notFilters;
            }

            if (!notFilters.length && filedExists) {
                query.query.bool.must_not = [{ exists: { field: filedExists } }];
            }
            if (notFilters.length && filedExists) {
                query.query.bool.must_not.push({ exists: { field: filedExists } });
            }
        }

        if (!filters.length && !notFilters.length && !options.q) {
            query.query.match_all = {};
        }

        if (options.count) {
            query.size = options.count;
        }

        // Add parameter "from" instead of "search_after"
        if (options.after) {
            query.from = options.after;
        }

        // add sorting
        query.sort = [];
        if (options.sort) {
            options.sort.forEach(sort => {
                let sortField = sort.split("(")[0];
                query.sort.push({ [sortField]: sort.toUpperCase().endsWith("(DESC)") ? "desc" : "asc" });
            });
        }
        query.sort.push({ id: "asc" })

        return query;
    }

    search(options) {
        let query = Promise.resolve();
        if (options.after) {
            let request = this.createRequest(options);
            // request.query.bool = request.query.bool || {};
            // request.query.bool.filter = {
            //     term: {
            //         id: options.after
            //     }
            // };
            query = this.elastic.search({
                index: options.object,
                type: options.object,
                body: request
            }).then(body => {
                if (body.hits.hits.length) {
                    return body.hits.hits[0].sort;
                }
                throw new Error("Incorrect parameter 'after'");
            });
        }

        return query.then(searchAfter => {
            let request = this.createRequest(options);
            // Fix default sort by id for npi search
            if (options.object === "log_line") {
                request.sort.splice(request.sort.findIndex(el => el.hasOwnProperty('id')));
                request.sort.push({timestamp: "desc"});
            }
            if (options.object === "npi_location" || options.object === "npi_entity") {
                request.sort.splice(request.sort.findIndex(el => el.hasOwnProperty('id')));
                request.sort.push({npi: "asc"});
            }
            return this.elastic.search({
                index: options.object,
                type: options.object,
                body: request
            });
        }).then(body => {
            // Fix for npi search
            if (options.object === "npi_location" || options.object === "npi_entity") {
                let res = {
                    results: body.hits.hits.map(doc => doc._source),
                    count: body.hits.total
                };
                return res;
            }

            if (options.object === "log_line") {
                let res = {
                    results: body.hits.hits.map(doc => doc._source),
                    count: body.hits.total
                };
                return res;
            }
            // Regular search
            let res = {
                results: body.hits.hits.map(doc => doc._id),
                count: body.hits.total
            };
            if (body.hits.hits.length > 0) {
                res.first = body.hits.hits[0]._source.id;
                res.last = body.hits.hits.slice(-1)[0]._source.id;
            }
            return res;
        });
    }
}

module.exports.Searcher = Searcher;
