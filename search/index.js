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
        Object.keys(options.filters || {}).forEach(key => {
            let val = options.filters[key];
            if (val.length === 1) {
                val = val[0];
            }

            if (val === "-") {
                filters.push({missing: {field: key}});
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
                if (eqVals.length) {
                    filters.push({terms: {[key]: eqVals}});
                }

                let notEqVals = val.filter(val => val.startsWith("!")).map(val => val.slice(1));
                if (notEqVals.length) {
                    notFilters.push({terms: {[key]: notEqVals}});
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

        let query = {query: {}};

        if (options.fields && options.q) {
            query.query.bool = query.query.bool || {};
            query.query.bool.should = {
                multi_match: {
                    query: options.q,
                    fields: options.fields
                }
            };
        }

        if (filters.length || notFilters.length) {
            query.query.bool = query.query.bool || {};
            if (filters.length) {
                query.query.bool.must = filters;
            }
            if (notFilters.length) {
                query.query.bool.must_not = notFilters;
            }
        }

        if (!filters.length && !options.q) {
            query.query.match_all = {};
        }

        if (options.count) {
            query.size = options.count;
        }

        // add sorting
        query.sort = [];
        if (options.sort) {
            options.sort.forEach(sort => {
                let sortField = sort.split("(")[0];
                query.sort.push({[sortField]: sort.toUpperCase().endsWith("(DESC)") ? "desc" : "asc"});
            });
        }
        query.sort.push({id: "asc"})

        return query;
    }

    search(options) {
        let query = Promise.resolve();
        if (options.after) {
            let request = this.createRequest(options);
            request.query.bool = request.query.bool || {};
            request.query.bool.filter = {
                term: {
                    id: options.after
                }
            };
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
            if (searchAfter) {
                request.search_after = searchAfter;
            }
            return this.elastic.search({
                index: options.object,
                type: options.object,
                body: request
            });
        }).then(body => {
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
