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
            query.query.multi_match = {
                query: options.q,
                fields: options.fields
            };
        }

        if (filters.length || notFilters.length) {
            query.query.bool = {
                must: filters,
                must_not: notFilters
            };
        }

        if (options.count) {
            query.size = options.count;
        }

        if (options.after) {
            query.from = options.after;
        }

        // add sorting
        if (options.sort) {
            query.sort = [];
            options.sort.forEach(sort => {
                let sortField = sort.split("(")[0];
                query.sort.push({[sortField]: sort.toUpperCase().endsWith("(DESC)") ? "desc" : "asc"});
            });
        }

        return query;
    }

    search(options) {
        return this.elastic.search({
            index: options.object,
            type: options.object,
            body: this.createRequest(options)
        }).then(body => ({results: body.hits.hits.map(doc => doc._id), count: body.hits.total}));
    }
}

module.exports.Searcher = Searcher;
