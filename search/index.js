"use strict";

const elasticsearch = require("elasticsearch");

let dateRegExp = /^\d{4}-\d{2}-\d{2}$/;

/* eslint camelcase: 0 */

class Searcher {
    constructor(config) {
        this.elastic = new elasticsearch.Client(config);
    }
    parseFilter(filters) {
        if (!(Object.keys(filters)).includes("AND") && !(Object.keys(filters)).includes("OR")) {
            return this.parseFiltersFromService(filters);
        }
        const res = { bool: {} };
        Object.keys(filters).map(item => {
            let operator;
            if (item === "AND") operator = "must";
            else if (item === "OR") operator = "should";
            res.bool[operator] = [];
            Object.keys(filters[item]).map(key => {
                if (key === "AND" || key === "OR") {
                    const resSub = this.parseFilter({ [key]: filters[item][key] });
                    res.bool[operator].push(resSub);
                    return;
                }
                if (filters[item][key][0] === "!") res.bool[operator].push({ bool: { must_not: { exists: { field: key } } } });
                else if (filters[item][key][0] === "") res.bool[operator].push({ exists: { field: key } });
                else filters[item][key].map(item => {
                    if (item[0] === "!") res.bool[operator].push({ bool: { must_not: { term: { [key]: item.slice(1) } } } });
                    else if (item.toLowerCase() === "exists") res.bool[operator].push({ bool: { must: { exists: { field: key } } } })
                    else res.bool[operator].push({ term: { [key]: item } })
                });
            });
        });
        return res;
    }
    parseFiltersFromService(filters) {
        const res = { bool: { must: [] } };
        for (const field of Object.keys(filters)) {
            if (filters[field] === "!") res.bool.must.push({ bool: { must_not: { exists: { field: field } } } });
            else if (filters[field] === "") res.bool.must.push({ exists: { field: field } });
            else res.bool.must.push({ term: { [field]: filters[field] } });
        }
        return res;
    }
    createRequest(options) {
        let filters = [];
        let sort_by_val = options.sort_by.split("(")[1].split(")")[0];
        let sort_by_field = options.sort_by.split("(")[0];

        if (options.filters) {
            filters = this.parseFilter(options.filters);
        }

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
        const filterRange = [];
        if (Object.keys(range).length) {
            filterRange.push({ range: range });
        }
        let query;

        if ((options.object === "laboratory_reqs" || options.object === "distributor_reqs") && options.group_by) {
            query = {
                query: {},
                aggs: {}
            };

            query.aggs[options.object] = {
                terms: {
                    field: options.group_by
                }
            };
            if (options.sort_by && sort_by_val && sort_by_field) {
                if (sort_by_field === "result") {
                    query.aggs[options.object].terms.order = { _count: sort_by_val }
                }
                if (sort_by_field === "object_name") {
                    query.aggs[options.object].terms.order = { _key: sort_by_val }
                }
            }
        } else if ((options.object === "laboratory_reqs" || options.object === "distributor_reqs") && options.func === "sum" && !options.group_by) {
            query = {
                query: {},
                aggs: {}
            };

            query.aggs[options.object] = {
                sum: {
                    field: "tat"
                }
            };
        } else if ((options.object === "laboratory_reqs" || options.object === "distributor_reqs") && options.func === "average" && !options.group_by) {
            query = {
                query: {},
                aggs: {}
            };

            query.aggs[options.object] = {
                avg: {
                    field: "tat"
                }
            };
        } else {
            query = { query: {} };
        }

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
            options.q = options.q.replace("AND", "and");
            options.q = options.q.replace(/  +/g, ' ');
            options.q = options.q.split(" ").join(" AND ");
            query.query.bool.must = {
                query_string: {
                    query: `*${options.q}*`,
                    fields: options.fields
                }
            };
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
        query.sort.push({ id: "asc" });
        if (query.query.bool && query.query.bool.must) {
            if (filters.bool.must) {
                filters.bool.must.push(query.query.bool.must);
                query.query.bool = filters.bool;
            } else {
                const query_string = query.query.bool.must;
                query.query.bool.must = [query_string, { bool: filters.bool }];
            }
        } else {
            query.query.bool = filters.bool;
        }
        query.query.bool.filter = filterRange;
        return query;
    }

    search(options) {
        let query = Promise.resolve();
        if (options.after) {
            let request = this.createRequest(options);
            query = this.elastic.search({
                index: options.object,
                type: options.object,
                body: request
            }).then(body => {
                if (body.hits.hits.length) {
                    return body.hits.hits[0].sort;
                }
                // throw new Error("Incorrect parameter 'after'");
            });
        }
        return query.then(searchAfter => {
            let request = this.createRequest(options);
            // Fix default sort by id for npi search
            if (options.object === "log_line") {
                request.sort.splice(request.sort.findIndex(el => el.hasOwnProperty('id')));
                request.sort.push({ timestamp: "desc" });
            }
            if (options.object === "npi_entity") {
                request.sort.splice(request.sort.findIndex(el => el.hasOwnProperty('id')));
                request.sort.push({ npi_sort: "asc" });
            }
            if (options.object === "npi_location") {
                request.sort.splice(request.sort.findIndex(el => el.hasOwnProperty('id')));
                request.sort.push({ npi: "asc" });
            }

            return this.elastic.search({
                index: options.object,
                type: options.object,
                body: request
            });
        }).then(body => {
            // Fix for npi search
            if (options.object === "laboratory_reqs" || options.object === "distributor_reqs") {
                let res = {
                    results: body.hits.hits.map(doc => doc._source),
                    count: body.hits.total,
                    aggregations: body.aggregations ? body.aggregations : ""

                };
                return res;
            }
            if (options.object === "npi_location" || options.object === "npi_entity" || options.object === "log_line") {
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