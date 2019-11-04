"use strict";

const elasticsearch = require("elasticsearch");

/* eslint camelcase: 0 */

class Searcher {
    constructor(config) {
        this.elastic = new elasticsearch.Client(config);
    }
    parseFilter(filters, rec) {
        if (!(Object.keys(filters)).includes("AND") && !(Object.keys(filters)).includes("OR") && !rec) {
            return this.parseFiltersFromService(filters);
        }

        const operatorLogic = (Object.keys(filters))[0];
        let operator;
        if (operatorLogic === "AND") operator = "must";
        else if (operatorLogic === "OR") operator = "should";
        const res = { bool: { [operator]: [] } };
        filters[operatorLogic].forEach(item => {
            const objectKeys = Object.keys(item);
            objectKeys.forEach(key => {
                if (["AND", "OR"].includes(key)) res.bool[operator].push(this.parseFilter(item, true));
                else {
                    if (item[key] === "!") res.bool[operator].push({ bool: { must_not: { exists: { field: key } } } })
                    else if (item[key][0] === "!") res.bool[operator].push({ bool: { must_not: { term: { [key]: item[key].slice(1) } } } })
                    else if (item[key].toLowerCase() === "exists") res.bool[operator].push({ bool: { must: { exists: { field: key } } } })
                    else if (item[key] === "") res.bool[operator].push({ bool: { must: { exists: { field: key } } } })
                    else res.bool[operator].push({ term: { [key]: item[key] } })
                }

            })
        })
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
        if (options.sort_by) {
            sort_by_val = options.sort_by.split("(")[1].split(")")[0];
            sort_by_field = options.sort_by.split("(")[0];
        }

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
        let query = { query: {} };

        if (options.fields && options.q) {
            query.query.bool = query.query.bool || {};
            options.q = options.q.replace("AND", "and");
            options.q = options.q.replace(/  +/g, ' ');
            options.q = options.q.split(" ").join(" AND ");
            if (options.q.includes("/") || options.q.includes("(") || options.q.includes(")")) {
                query.query.bool.must = {
                    simple_query_string: {
                        query: `*${options.q}*`,
                        fields: options.fields
                    }
                };
            } else {
                query.query.bool.must = {
                    query_string: {
                        query: `*${options.q}*`,
                        fields: options.fields
                    }
                };
            }
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
        if (!options.notAddSort) query.sort.push({ id: "asc" });
        if (query.query.bool && query.query.bool.must) {
            if (!Array.isArray(filters) && filters.bool.must) {
                filters.bool.must.push(query.query.bool.must);
                query.query.bool = filters.bool;
            } else {
                const query_string = query.query.bool.must;
                query.query.bool.must = [query_string, { bool: filters.bool }];
            }
        } else {
            query.query.bool = filters.bool;
        }
        if (query.query.bool) query.query.bool.filter = filterRange;
        else {
            query.query.bool = { filter: [] };
        }

        if (query.query.bool && query.query.bool.must) {
            query.query.bool.must = query.query.bool.must.filter(item => {
                if (!Object.keys(item).length) return false;
                return !Object.keys(item).every(key => !item[key]);
            });
            query.query.bool.must.forEach(item => console.log(item));
        }
        return query;
    }

    search(options) {
        let query = Promise.resolve();
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
            if (options.object === "waystar_payer") {
                request.sort.splice(request.sort.findIndex(el => el.hasOwnProperty('id')));
                request.sort.push({ name_sort: "asc" });
            }

            const aliases = ["roles"];
            const type = aliases.includes(options.object) ? null : options.object;
            console.log("???>>>>>>>>>>>>>", request.query.bool.must);

            return this.elastic.search({
                index: options.object,
                type,
                body: request
            });
        }).then(body => {
            // Fix for npi search
            const returnObject = [
                "npi_location",
                "npi_entity",
                "log_line",
                "lab_test_position_v2",
                "lab_test_position_test2",
                "lab_test_position_test1",
                "waystar_payer",
                "zip_code"];
            if (returnObject.includes(options.object)) {
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