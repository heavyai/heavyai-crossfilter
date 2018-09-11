"use strict"
import chai, { expect } from "chai"

const cf = require("../src/mapd-crossfilter")

import spies from "chai-spies"
import { replaceRelative } from "../src/mapd-crossfilter"
chai.use(spies)

// TODO either remove or fix the append options to filters
describe("crossfilter", () => {
  let isPST
  let crossfilter
  let getFieldsReturnValue
  const getFields = (name, callback) =>
    setTimeout(() => callback(null, getFieldsReturnValue), 0)
  beforeEach(() => {
    getFieldsReturnValue = []
    crossfilter = cf.crossfilter()
    const date = new Date("1/1/2006")
    isPST =
      date.toString() === "Sun Jan 01 2006 00:00:00 GMT-0800 (PST)"
        ? true
        : false
  })
  it("has a version", () => {
    expect(cf.crossfilter.version).to.eq("1.3.11")
  })
  it("has a type", () => {
    expect(crossfilter.type).to.eq("crossfilter")
  })
  it("has an id", () => {
    expect(crossfilter.getId()).to.be.at.least(0)
  })
  it("can be invoked to setDataAsync and return self", function() {
    const dataConnector = { getFields }
    const dataTables = "a table"
    return cf.crossfilter(dataConnector, dataTables).then(crsfltr => {
      expect(crsfltr.type).to.eql("crossfilter")
      expect(crsfltr.getTable()).to.eql(["a table"])
    })
  })
  describe(".setDataAsync", function() {
    it("selects from multiple tables", function() {
      const dataConnector = {
        getFields,
        query: (a, b, c) => c(null, [{ n: 1 }])
      }
      const dataTables = ["tableA", "tableB"]
      return crossfilter
        .setDataAsync(dataConnector, dataTables)
        .then(crossfilter.sizeAsync)
        .then(function() {
          expect(crossfilter.peekAtCache().cache).to.have.key(
            "SELECT COUNT(*) as n FROM tableA,tableB"
          )
        })
    })
    it("joins tables", () => {
      const dataConnector = { getFields, query: _ => _ }
      const dataTables = "table1"
      const joinAttrs = [
        { table1: "table1", table2: "table2", attr1: "id", attr2: "x_id" }
      ]
      crossfilter.setDataAsync(dataConnector, dataTables, joinAttrs)
      crossfilter.size()
      expect(crossfilter.peekAtCache().cache).to.have.key(
        "SELECT COUNT(*) as n FROM table1 WHERE table1.id = table2.x_id"
      )
    })
    it("joins multiple tables", () => {
      const dataConnector = { getFields, query: _ => _ }
      const dataTables = "table1"
      const joinAttrs = [
        { table1: "table1", table2: "table2", attr1: "id", attr2: "x_id" },
        { table1: "table2", table2: "table3", attr1: "id", attr2: "y_id" }
      ]
      crossfilter.setDataAsync(dataConnector, dataTables, joinAttrs)
      crossfilter.size()
      expect(crossfilter.peekAtCache().cache).to.have.key(
        "SELECT COUNT(*) as n FROM table1 WHERE table1.id = table2.x_id AND table2.id = table3.y_id"
      )
    })
    xit("joins the same way regardless of order", () => {
      const dataConnector = { getFields, query: _ => _ }
      const dataTables = "table1"
      const joinAttrs = [
        { table1: "table2", table2: "table1", attr1: "x_id", attr2: "id" },
        { table1: "table1", table2: "table2", attr1: "id", attr2: "x_id" }
      ]
      crossfilter.setDataAsync(dataConnector, dataTables, joinAttrs)
      crossfilter.size()
      expect(crossfilter.peekAtCache().cache).to.have.key(
        "SELECT COUNT(*) as n FROM table1 WHERE table1.id = table2.x_id AND table2.id = table3.y_id"
      )
    })
    it("identifies ambiguous table columns", () => {
      const columnsArray = [
        { name: "age", type: "idk", is_array: false, is_dict: false, precision: 0 },
        { name: "age", type: "other", is_array: false, is_dict: false, precision: 0 }
      ]
      getFieldsReturnValue = columnsArray
      const dataConnector = { getFields }
      return crossfilter.setDataAsync(dataConnector, "tableA").then(() => {
        expect(crossfilter.getColumns()).to.eql({
          "tableA.age": {
            table: "tableA",
            column: "age",
            type: "other",
            is_array: false,
            is_dict: false,
            name_is_ambiguous: true,
            precision: 0
          }
        })
      })
    })
  })
  describe(".filter", () => {
    let filter
    let globalFilter
    beforeEach(() => {
      filter = crossfilter.filter()
      globalFilter = crossfilter.filter(true)
    })
    describe(".filter", () => {
      it("adds a filter", () => {
        filter.filter("x")
        expect(crossfilter.getFilter()).to.eql(["x"])
      })

      it("adds a global filter", () => {
        globalFilter.filter("x")
        expect(crossfilter.getGlobalFilter()).to.eql(["x"])
      })

      it("filters all if argument undefined or null", () => {
        filter.filter()
        expect(crossfilter.getFilter()).to.eql([""])
        filter.filter(null)
        expect(crossfilter.getFilter()).to.eql([""])
      })
      it("can overwrite or append filters", () => {
        filter.filter("x")
        expect(crossfilter.getFilter()).to.eql(["x"])
        filter.filter("y")
        expect(crossfilter.getFilter()).to.eql(["y"])
        crossfilter.filter().filter("z")
        expect(crossfilter.getFilter()).to.eql(["y", "z"])
      })
    })
    describe(".filterAll", () => {
      it("adds empty string to filters", () => {
        filter.filterAll()
        expect(crossfilter.getFilter()).to.eql([""])
      })

      it("adds empty string to globalFilters", () => {
        globalFilter.filterAll()
        expect(crossfilter.getFilter()).to.eql([""])
      })
    })
    describe(".getFilter", () => {
      it("returns current filter", () => {
        const filter = crossfilter.filter()
        filter.filter("a")
        filter.filter("b")
        expect(filter.getFilter()).to.eq("b")
      })

      it("returns current global filter", () => {
        const filter = crossfilter.filter(true)
        filter.filter("a")
        filter.filter("b")
        expect(filter.getFilter()).to.eq("b")
      })
    })
    describe(".toggleTarget", () => {
      it("returns the filter object", () => {
        expect(filter.toggleTarget()).to.eq(filter)
      })
      it("toggles between target filter and null", () => {
        expect(filter.getTargetFilter()).to.eq(null)
        filter.toggleTarget()
        expect(filter.getTargetFilter()).to.eq(0)
        filter.toggleTarget()
        expect(filter.getTargetFilter()).to.eq(null)
      })
    })
  })

  describe(".getColumns", () => {
    it("keeps track of table columns", function() {
      const columnsArray = [
        { name: "age", type: "idk", is_array: false, is_dict: false, precision: 0 },
        { name: "sex", type: "idk", is_array: false, is_dict: false, precision: 0 }
      ]
      getFieldsReturnValue = columnsArray
      const dataConnector = { getFields }
      return crossfilter.setDataAsync(dataConnector, "tableA").then(() => {
        expect(crossfilter.getColumns()).to.eql({
          "tableA.age": {
            table: "tableA",
            column: "age",
            type: "idk",
            is_array: false,
            is_dict: false,
            name_is_ambiguous: false,
            precision: 0
          },
          "tableA.sex": {
            table: "tableA",
            column: "sex",
            type: "idk",
            is_array: false,
            is_dict: false,
            name_is_ambiguous: false,
            precision: 0
          }
        })
      })
    })
  })
  describe(".dimension", () => {
    let dimension
    beforeEach(function() {
      return crossfilter.setDataAsync({ getFields }, []).then(crsfltr => {
        crossfilter = crsfltr
        dimension = crsfltr.dimension("bargle")
      })
    })
    it("returns itself", () => {
      expect(crossfilter.dimension().type).to.eq("dimension")
    })
    describe(".order", () => {
      it("returns own dimension object", () => {
        expect(dimension.order()).to.eq(dimension)
      })
      it("sets orderExpression", function() {
        const dataConnector = { getFields, query: _ => _ }
        return crossfilter
          .setDataAsync(dataConnector, "table1")
          .then(crsfltr => {
            dimension = crsfltr.dimension("bargle")
            dimension.order("created_at")
            dimension.projectOnAllDimensions(true)
            expect(dimension.top(1, 1)).to.eq(
              "SELECT bargle FROM table1 ORDER BY created_at DESC LIMIT 1 OFFSET 1"
            )
          })
      })
    })
    describe(".orderNatural", () => {
      it("returns own dimension object", () => {
        expect(dimension.orderNatural()).to.eq(dimension)
      })
      it("nulls out orderExpression", function() {
        const dataConnector = { getFields, query: _ => _ }
        return crossfilter
          .setDataAsync(dataConnector, "table1")
          .then(crsfltr => {
            dimension = crsfltr.dimension("bargle")
            dimension.projectOnAllDimensions(true)
            dimension.order("created_at")
            expect(dimension.top(1, 1)).to.eq(
              "SELECT bargle FROM table1 ORDER BY created_at DESC LIMIT 1 OFFSET 1"
            )
            dimension.orderNatural()
            expect(dimension.top(1, 1)).to.eq(
              "SELECT bargle FROM table1 ORDER BY bargle DESC LIMIT 1 OFFSET 1"
            )
          })
      })
    })
    describe(".selfFilter", () => {
      it("returns own dimension object if arguments", () => {
        expect(dimension.selfFilter("admin = true")).to.eq(dimension)
      })
      it("returns current selfFilter if no arguments", () => {
        expect(dimension.selfFilter()).to.eq(null)
        expect(dimension.selfFilter("admin = true")).to.eq(dimension)
        expect(dimension.selfFilter()).to.eq("admin = true")
      })
      it("filters query", function() {
        const dataConnector = { getFields, query: _ => _ }
        return cf.crossfilter(dataConnector, "table1").then(crsfltr => {
          dimension = crsfltr.dimension("age")
          dimension.selfFilter("admin = true")
          dimension.projectOnAllDimensions(true)
          expect(dimension.top(1, 1)).to.eq(
            "SELECT age FROM table1 WHERE admin = true ORDER BY age DESC LIMIT 1 OFFSET 1"
          )
        })
      })
      it("appends to existing filters", function() {
        const dataConnector = { getFields, query: _ => _ }
        return cf.crossfilter(dataConnector, "table1").then(crsfltr => {
          dimension = crsfltr.dimension("age")
          dimension.selfFilter("admin = true")
          dimension.filter(35)
          dimension.projectOnAllDimensions(true)
          expect(dimension.top(1, 1)).to.eq(
            "SELECT age FROM table1 WHERE age = 35 AND admin = true ORDER BY age DESC LIMIT 1 OFFSET 1"
          )
        })
      })
    })
    describe(".filter", () => {
      it("escapes apostrophes in filter values", () => {
        dimension.filterExact("McDonald's")
        expect(dimension.getFilterString()).to.eq("bargle = 'McDonald''s'")
        dimension.filterAll(null)
        dimension.filterExact("'til later")
        expect(dimension.getFilterString()).to.eq("bargle = '''til later'")
        dimension.filterAll(null)
        dimension.filterExact("ye ol'")
        expect(dimension.getFilterString()).to.eq("bargle = 'ye ol'''")
        dimension.filterAll(null)
        dimension.filterExact("Wouldn't've")
        expect(dimension.getFilterString()).to.eq("bargle = 'Wouldn''t''ve'")
      })
      it("escapes percent signs in filter values", () => {
        dimension.filterExact("100%")
        expect(dimension.getFilterString()).to.eq("bargle = '100%'")
        dimension.filterAll(null)
        dimension.filterExact("% change")
        expect(dimension.getFilterString()).to.eq("bargle = '% change'")
        dimension.filterAll(null)
        dimension.filterExact("%%")
        expect(dimension.getFilterString()).to.eq("bargle = '%%'")
      })
      it("escapes underscores in filter values", () => {
        dimension.filterExact("100_")
        expect(dimension.getFilterString()).to.eq("bargle = '100_'")
        dimension.filterAll(null)
        dimension.filterExact("_ change")
        expect(dimension.getFilterString()).to.eq("bargle = '_ change'")
        dimension.filterAll(null)
        dimension.filterExact("__")
        expect(dimension.getFilterString()).to.eq("bargle = '__'")
      })
      it("returns filter IS NULL if range is null", () => {
        dimension.filterExact("something")
        expect(dimension.getFilterString()).to.eq("bargle = 'something'")
        dimension.filterAll(null)
        expect(dimension.getFilterString()).to.eq("")
        dimension.filterExact("something")
        expect(dimension.getFilterString()).to.eq("bargle = 'something'")
        dimension.filter(null, null, null, null)
        expect(dimension.getFilterString()).to.eq("bargle IS NULL")
      })
      it("returns filterAll if range is undefined", () => {
        dimension.filterExact("something")
        expect(dimension.getFilterString()).to.eq("bargle = 'something'")
        dimension.filterAll(null)
        expect(dimension.getFilterString()).to.eq("")
        dimension.filterExact("something")
        expect(dimension.getFilterString()).to.eq("bargle = 'something'")
        dimension.filter(undefined)
        expect(dimension.getFilterString()).to.eq("")
      })
      it("returns filterRange if range is array and multiDim is falsey", () => {
        dimension.filterRange([1, 2], undefined, undefined)
        expect(dimension.getFilterString()).to.eq(
          "(bargle >= 1 AND bargle <= 2)"
        )
        dimension.filter([1, 2], null, null)
        expect(dimension.getFilterString()).to.eq(
          "(bargle >= 1 AND bargle <= 2)"
        )
      })
      it("returns filterExact (range isn't array or multiDim is truthy) and range isn't a function", () => {
        dimension.filterExact("a range", undefined)
        expect(dimension.getFilterString()).to.eq("bargle = 'a range'")
        dimension.filter("a range", null, null)
        expect(dimension.getFilterString()).to.eq("bargle = 'a range'")
      })
    })

    describe(".filterRelative", () => {
      it("converts range array", () => {
        dimension.filterRelative([{ now: true }, { now: true }])
        expect(dimension.getFilterString()).to.eq(
          "(bargle >= NOW() AND bargle <= NOW())"
        )
      })

      it("converts range array with relative times", () => {
        dimension.filterRelative([
          { datepart: "days", number: -1 },
          { now: true }
        ])
        expect(dimension.getFilterString()).to.eq(
          "(bargle >= DATE_ADD(days, -1, NOW()) AND bargle <= NOW())"
        )
      })

      it("returns the value if not value is not relative object", () => {
        dimension.filterRelative(["foo", "bar"])
        expect(dimension.getFilterString()).to.eq(
          "(bargle >= 'foo' AND bargle <= 'bar')"
        )
      })
    })
    describe(".filterExact", () => {
      it("returns own dimension object", () => {
        expect(dimension.filterExact()).to.eql(dimension)
      })
      it("converts dates", () => {
        dimension = crossfilter.dimension(["age", "sex", "created_at"])
        dimension.filterExact([50, "f", new Date("2016-01-01")])
        expect(dimension.getFilterString()).to.eq(
          "age = 50 AND sex = 'f' AND created_at = TIMESTAMP(0) '2016-01-01 00:00:00'"
        )
      })
      it("uses ANY if dim contains array", function() {
        const columnsArray = [
          { name: "age", type: "idk", is_array: true, is_dict: false }
        ]
        getFieldsReturnValue = columnsArray
        return crossfilter
          .setDataAsync({ getFields }, "tableA")
          .then(crsfltr => {
            dimension = crsfltr.dimension(["tableA.age", "sex", "created_at"])
            dimension.filterExact([50, "f", new Date("2016-01-01")])
            expect(dimension.getFilterString()).to.eq(
              "50 = ANY tableA.age AND sex = 'f' AND created_at = TIMESTAMP(0) '2016-01-01 00:00:00'"
            )
          })
      })
      it("does not use ANY if dim does not contain array", () => {
        dimension.filterExact([50])
        expect(dimension.getFilterString()).to.eq("bargle = 50")
      })
      it("sets the current filter if not appending", () => {
        dimension = crossfilter.dimension(["age", "sex"])
        dimension.filterExact([50, "f"])
        dimension.filterExact([100, "m"])
        expect(dimension.getFilterString()).to.eq("age = 100 AND sex = 'm'")
      })
      it("adds to current filter if appending", () => {
        dimension = crossfilter.dimension(["age", "sex"])
        dimension.filterExact([50, "f"])
        dimension.filterExact([100, "m"], true)
        expect(dimension.getFilterString()).to.eq(
          "age = 50 AND sex = 'f'age = 100 AND sex = 'm'"
        ) // TODO invalid SQL? yes
      })
      it("uses greater and less than when value is a range array", () => {
        dimension = crossfilter.dimension(["airtime", "carrier_name"])
        dimension.filterExact([[337.5, 450], "United Air Lines"])
        expect(dimension.getFilterString()).to.eq(
          "airtime >= 337.5 AND airtime <= 450 AND carrier_name = 'United Air Lines'"
        )
      })
      it("should handle cases where there is a extract binParam", () => {
        dimension = crossfilter.dimension(["contrib_date"])
        const binParams = [
          {
            timeBin: "day",
            extract: true,
            numBins: 400,
            binBounds: [new Date(), new Date()]
          }
        ]
        dimension.group().binParams(binParams)
        dimension.filterExact(17, false, false, binParams)
        expect(dimension.getFilterString()).to.equal(
          "extract(day from contrib_date) = 17"
        )
      })
    })
    describe(".filterRange", () => {
      it("returns own dimension object", () => {
        expect(dimension.filterRange(true)).to.eql(dimension)
      })
      it("sets filterVal", () => {
        dimension.filterRange(123)
        expect(dimension.getFilter()).to.eql([123])
      })
      it("sets the current filter if not appending", () => {
        dimension.filterRange([1, 2])
        dimension.filterRange([3, 4], false)
        expect(dimension.getFilterString()).to.eq(
          "(bargle >= 3 AND bargle <= 4)"
        )
      })
      it("adds to current filter if appending", () => {
        dimension.filterRange([1, 2])
        dimension.filterRange([3, 4], true)
        expect(dimension.getFilterString()).to.eq(
          "(bargle >= 1 AND bargle <= 2)(bargle >= 3 AND bargle <= 4)"
        ) // TODO valid SQL?
      })
      it("converts range to list if not already one", () => {
        dimension.filterRange("not a list")
        expect(dimension.getFilter()).to.eql(["not a list"])
      })
      xit("resets range if resetRange truthy", () => {
        dimension.filterRange([1, 2])
        expect(dimension.getFilter()).to.eql([[1, 2]])
        expect(dimension.getFilterString()).to.eq(
          "(bargle >= 1 AND bargle <= 2)"
        )
        dimension.filterRange([3, 4], false, false)
        expect(dimension.getFilter()).to.eql([[3, 4]])
        expect(dimension.getFilterString()).to.eq(
          "(bargle >= 1 AND bargle <= 2)"
        )
      })
      it("combines subExpressions with AND", () => {
        dimension = crossfilter.dimension(["argle", "bargle"])
        dimension.filterRange([[1, 2], [3, 4]])
        expect(dimension.getFilterString()).to.eq(
          "(argle >= 1 AND argle <= 2 AND bargle >= 3 AND bargle <= 4)"
        )
      })
      it("inverts the subExpression if isFilterInverse is true", () => {
        dimension.filterRange([3, 4], false, false, true)
        expect(dimension.getFilterString()).to.eq(
          "(NOT(bargle >= 3 AND bargle <= 4))"
        )
      })
      it("handles range when bin param is extract", () => {
        const binParams = [
          {
            binBounds: [new Date("1/1/2006"), new Date("1/1/2007")],
            numBins: 400,
            timeBin: "month",
            extract: true
          }
        ]
        dimension.group().binParams(binParams)
        dimension.filterRange([1, 2], false, false, false, binParams)
        expect(dimension.getFilterString()).to.eq(
          "(extract(month from bargle) >= 1 AND extract(month from bargle) <= 2)"
        )
      })
    })
    describe(".filterAll", () => {
      it("returns own dimension object", () => {
        expect(dimension.filterAll(true)).to.eql(dimension)
      })
      it("does not trigger a jQuery 'filter-clear' if softFilterClear is truthy", () => {
        expect(dimension.filterAll(true)).to.eql(dimension)
      })
      it("nulls out filterVal and the current filter", () => {
        dimension.filterRange(123)
        dimension.filterLike("bob")
        expect(dimension.getFilter()).to.eql([123])
        expect(dimension.getFilterString()).to.eq("bargle like '%bob%'")
        dimension.filterAll(true)
        expect(dimension.getFilter()).to.eq(null)
        expect(dimension.getFilterString()).to.eq("")
      })
    })
    describe(".filterMulti", () => {
      it("returns own dimension object", () => {
        expect(dimension.filterMulti([], null, null)).to.eql(dimension)
      })
      it("AND concats if drillDownFilter set and inverseFilters is true", () => {
        dimension.setDrillDownFilter("drilldown is true")
        expect(dimension.filterMulti([1, 2], null, true)).to.eql(dimension)
        expect(dimension.getFilterString()).to.eq(
          "(NOT (bargle = 1) AND NOT (bargle = 2))"
        )
      })
      it("OR concats if drillDownFilter not set", () => {
        expect(dimension.filterMulti([1, 2], null, undefined)).to.eql(dimension)
        expect(dimension.getFilterString()).to.eq("(bargle = 1 OR bargle = 2)")
      })
    })
    describe(".filterLike", () => {
      it("escapes apostrophes in filter values", () => {
        dimension.filterLike("McDonald's")
        expect(dimension.getFilterString()).to.eq("bargle like '%McDonald''s%'")
        dimension.filterAll(null)
        dimension.filterLike("'til later")
        expect(dimension.getFilterString()).to.eq("bargle like '%''til later%'")
        dimension.filterAll(null)
        dimension.filterLike("ye ol'")
        expect(dimension.getFilterString()).to.eq("bargle like '%ye ol''%'")
        dimension.filterAll(null)
        dimension.filterLike("Wouldn't've")
        expect(dimension.getFilterString()).to.eq(
          "bargle like '%Wouldn''t''ve%'"
        )
      })
      it("escapes percent signs in filter values", () => {
        dimension.filterLike("100%")
        expect(dimension.getFilterString()).to.eq("bargle like '%100\\%%'")
        dimension.filterAll(null)
        dimension.filterLike("% change")
        expect(dimension.getFilterString()).to.eq("bargle like '%\\% change%'")
        dimension.filterAll(null)
        dimension.filterLike("%%")
        expect(dimension.getFilterString()).to.eq("bargle like '%\\%\\%%'")
      })
      it("escapes underscores in filter values", () => {
        dimension.filterLike("100_")
        expect(dimension.getFilterString()).to.eq("bargle like '%100\\_%'")
        dimension.filterAll(null)
        dimension.filterLike("_ change")
        expect(dimension.getFilterString()).to.eq("bargle like '%\\_ change%'")
        dimension.filterAll(null)
        dimension.filterLike("__")
        expect(dimension.getFilterString()).to.eq("bargle like '%\\_\\_%'")
      })
      it("sets like filter if none already exists", () => {
        dimension.filterLike("bob")
        expect(dimension.getFilterString()).to.eq("bargle like '%bob%'")
      })
      it("adds like filter if one already exists", () => {
        dimension.filterLike("bob")
        dimension.filterLike("marl", true)
        expect(dimension.getFilterString()).to.eq(
          "bargle like '%bob%'bargle like '%marl%'"
        ) // TODO this doesn't look like proper SQL
      })
    })
    describe(".filterILike", () => {
      it("escapes apostrophes in filter values", () => {
        dimension.filterILike("McDonald's")
        expect(dimension.getFilterString()).to.eq(
          "bargle ilike '%McDonald''s%'"
        )
        dimension.filterAll(null)
        dimension.filterILike("'til later")
        expect(dimension.getFilterString()).to.eq(
          "bargle ilike '%''til later%'"
        )
        dimension.filterAll(null)
        dimension.filterILike("ye ol'")
        expect(dimension.getFilterString()).to.eq("bargle ilike '%ye ol''%'")
        dimension.filterAll(null)
        dimension.filterILike("Wouldn't've")
        expect(dimension.getFilterString()).to.eq(
          "bargle ilike '%Wouldn''t''ve%'"
        )
      })
      it("escapes percent signs in filter values", () => {
        dimension.filterILike("100%")
        expect(dimension.getFilterString()).to.eq("bargle ilike '%100\\%%'")
        dimension.filterAll(null)
        dimension.filterILike("% change")
        expect(dimension.getFilterString()).to.eq("bargle ilike '%\\% change%'")
        dimension.filterAll(null)
        dimension.filterILike("%%")
        expect(dimension.getFilterString()).to.eq("bargle ilike '%\\%\\%%'")
      })
      it("escapes underscores in filter values", () => {
        dimension.filterILike("100_")
        expect(dimension.getFilterString()).to.eq("bargle ilike '%100\\_%'")
        dimension.filterAll(null)
        dimension.filterILike("_ change")
        expect(dimension.getFilterString()).to.eq("bargle ilike '%\\_ change%'")
        dimension.filterAll(null)
        dimension.filterILike("__")
        expect(dimension.getFilterString()).to.eq("bargle ilike '%\\_\\_%'")
      })
      it("sets like filter if none already exists", () => {
        dimension.filterILike("bob")
        expect(dimension.getFilterString()).to.eq("bargle ilike '%bob%'")
      })

      it("appends like filter if append flag is true", () => {
        dimension.filterILike("bob", true)
        expect(dimension.getFilterString()).to.eq("bargle ilike '%bob%'")
      })
    })

    describe(".filterNotILike", () => {
      it("sets not like filter if none already exists", () => {
        dimension.filterNotILike("bob")
        expect(dimension.getFilterString()).to.eq("NOT( bargle ilike '%bob%')")
      })

      it("appends not like filter if append flag is true", () => {
        dimension.filterNotILike("bob", true)
        expect(dimension.getFilterString()).to.eq("NOT( bargle ilike '%bob%')")
      })
    })
    describe(".filterIsNull", () => {
      it("returns own dimension object", () => {
        expect(dimension.filterIsNull()).to.eql(dimension)
      })
      it("creates the filter", () => {
        dimension.filterIsNull()
        expect(dimension.getFilterString()).to.eq("bargle IS NULL")
      })
      it("appends the filter", () => {
        dimension.filter("foo")
        dimension.filterIsNull(true)
        expect(dimension.getFilterString()).to.eq(
          "bargle = 'foo'bargle IS NULL"
        )
      })
    })
    describe(".filterIsNotNull", () => {
      it("returns own dimension object", () => {
        expect(dimension.filterIsNotNull()).to.eql(dimension)
      })
      it("creates the filter", () => {
        dimension.filterIsNotNull()
        expect(dimension.getFilterString()).to.eq("bargle IS NOT NULL")
      })
      it("appends the filter", () => {
        dimension.filter("foo")
        dimension.filterIsNotNull(true)
        expect(dimension.getFilterString()).to.eq(
          "bargle = 'foo'bargle IS NOT NULL"
        )
      })
    })
    describe(".getCrossfilter", () => {
      it("returns parent crossfilter", () => {
        expect(dimension.getCrossfilter()).to.eq(crossfilter)
      })
    })
    describe(".getCrossfilterId", () => {
      it("returns parent crossfilter's id", () => {
        expect(dimension.getCrossfilterId()).to.eq(crossfilter.getId())
      })
    })
    describe(".getFilter", () => {
      it("returns filterVal", () => {
        expect(dimension.getFilter()).to.eq(null)
        dimension.filterRange(123)
        expect(dimension.getFilter()).to.eql([123])
      })
    })
    describe(".getFilterString", () => {
      it("returns the filterString for the current dimensionIndex", () => {
        expect(dimension.getFilterString()).to.eq("")
        dimension.filterLike("bob")
        expect(dimension.getFilterString()).to.eq("bargle like '%bob%'")
      })
    })
    describe(".projectOn", () => {
      it("returns own dimension object", () => {
        expect(dimension.projectOn()).to.eq(dimension)
      })
      it("sets projectExpressions", () => {
        expect(dimension.getProjectOn()).to.eql([])
        dimension.projectOn(["id", "rx"])
        expect(dimension.getProjectOn()).to.eql(["id", "rx"])
      })
    })
    describe(".getProjectOn", () => {
      it("returns projectExpressions", () => {
        dimension.projectOn(["id", "rx"])
        expect(dimension.getProjectOn()).to.eql(["id", "rx"])
      })
    })
    describe(".getTable", () => {
      it("returns table", () => {
        expect(dimension.getTable()).to.eql(crossfilter.getTable())
      })
    })
    describe(".projectOnAllDimensions", () => {
      it("returns own dimension object", () => {
        expect(dimension.projectOnAllDimensions()).to.eq(dimension)
      })
      it("nulls out query if falsey", () => {
        dimension.projectOnAllDimensions(false)
        expect(dimension.top(1, 1)).to.deep.eq({})
      })
      it("allows query creation if truthy", function() {
        const dataConnector = { getFields, query: _ => _ }
        return crossfilter
          .setDataAsync(dataConnector, "table1")
          .then(crsfltr => {
            dimension = crsfltr.dimension("bargle")
            dimension.projectOnAllDimensions(true)
            expect(dimension.top(1, 1)).to.eq(
              "SELECT bargle FROM table1 ORDER BY bargle DESC LIMIT 1 OFFSET 1"
            )
          })
      })
    })
    describe(".multiDim", () => {
      it("should return whether dimension is multi", () => {
        const dataConnector = { getFields, query: _ => _ }
        return crossfilter
          .setDataAsync(dataConnector, "table1")
          .then(crsfltr => {
            expect(crsfltr.dimension(["argle", "bargle"]).multiDim()).to.equal(
              true
            )
          })
      })
      it("should set whether dimension is multi", () => {
        const dataConnector = { getFields, query: _ => _ }
        return crossfilter
          .setDataAsync(dataConnector, "table1")
          .then(crsfltr => {
            expect(
              crsfltr
                .dimension(["argle", "bargle"])
                .multiDim(false)
                .multiDim()
            ).to.equal(false)
          })
      })
    })
    describe(".samplingRatio", () => {
      it("sets samplingRatio to ratio", () => {
        dimension.samplingRatio(1)
        expect(dimension.getSamplingRatio()).to.eq(1)
        dimension.samplingRatio()
        expect(dimension.getSamplingRatio()).to.eq(undefined)
      })
    })
    describe(".writeTopQuery", () => {
      beforeEach(function() {
        const dataConnector = { getFields, query: _ => _ }
        return cf.crossfilter(dataConnector, "users").then(crsfltr => {
          crossfilter = crsfltr
          dimension = crossfilter.dimension("id")
          dimension.projectOnAllDimensions(true)
        })
      })
      it("returns empty string if no query", () => {
        dimension.projectOnAllDimensions(false)
        expect(dimension.writeTopQuery()).to.eql("")
      })
      it("constructs query", () => {
        expect(dimension.writeTopQuery(1, 2)).to.eq(
          "SELECT id FROM users ORDER BY id DESC LIMIT 1 OFFSET 2"
        )
      })
      it("orders by orderExpression if any", () => {
        dimension.order("custom")
        expect(dimension.writeTopQuery()).to.include("ORDER BY custom")
      })
      it("orders by dimensionExpression if no orderExpression", () => {
        expect(dimension.writeTopQuery()).to.include("ORDER BY id")
      })
      it("limits results if k < Infinity", () => {
        expect(dimension.writeTopQuery(10)).to.include("LIMIT 10")
      })
      it("does not limit result if k is Infinity", () => {
        expect(dimension.writeTopQuery(Infinity)).to.not.include("LIMIT")
      })
      it("can offset query", () => {
        expect(dimension.writeTopQuery(10, 20)).to.include("OFFSET 20")
      })
      it("can include a dimension's rowid", () => {
        expect(dimension.writeTopQuery(Infinity, 1, true)).to.eq(
          "SELECT id,users.rowid FROM users ORDER BY id DESC OFFSET 1"
        ) // TODO rowid squished
      })
      it("can use a samplingRatio < 1 with filterQuery", () => {
        dimension.samplingRatio(0.5)
        expect(dimension.writeTopQuery(Infinity)).to.eq(
          "SELECT id FROM users WHERE  MOD(users.rowid * 265445761, 4294967296) < 2147483648 ORDER BY id DESC"
        ) // TODO extra space between WHERE & MOD
      })
      it("can use a samplingRatio < 1 without filterQuery", () => {
        dimension.filterExact(1)
        dimension.samplingRatio(0.5)
        expect(dimension.writeTopQuery(Infinity)).to.eq(
          "SELECT id FROM users WHERE id = 1 AND  MOD(users.rowid * 265445761, 4294967296) < 2147483648 ORDER BY id DESC"
        ) // TODO extra space between AND & MOD
      })
      it("can use a join statement with no filterQuery or samplingRatio", () => {
        const dataConnector = { getFields }
        const dataTables = "tableA"
        const joinAttrs = [
          { table1: "table1", table2: "table2", attr1: "id", attr2: "x_id" }
        ]
        crossfilter.setDataAsync(dataConnector, dataTables, joinAttrs)
        dimension.samplingRatio(0.5)
        expect(dimension.writeTopQuery(Infinity)).to.eq(
          "SELECT id FROM tableA WHERE  MOD(tableA.rowid * 265445761, 4294967296) < 2147483648 AND table1.id = table2.x_id ORDER BY id DESC"
        ) // TODO extra space between WHERE & MOD
      })
      it("can use a joinStatement with no filterQuery and samplingRatio >= 1", () => {
        const dataConnector = { getFields }
        const dataTables = "tableA"
        const joinAttrs = [
          { table1: "table1", table2: "table2", attr1: "id", attr2: "x_id" }
        ]
        crossfilter.setDataAsync(dataConnector, dataTables, joinAttrs)
        dimension.samplingRatio(1.5)
        expect(dimension.writeTopQuery(Infinity)).to.eq(
          "SELECT id FROM tableA WHERE table1.id = table2.x_id ORDER BY id DESC"
        ) // TODO extra space between WHERE & MOD
      })
      it("AND concats multiple filters/dimensions", () => {
        dimension.filter([1, 2])
        dimension = crossfilter.dimension(["rx", "sex"])
        dimension.filter([3, 4])
        dimension.projectOnAllDimensions(true)
        expect(dimension.writeTopQuery(Infinity)).to.eq(
          "SELECT id,rx, sex FROM users WHERE (id >= 1 AND id <= 2) AND rx = 3 AND sex = 4 ORDER BY rx, sex DESC"
        ) // TODO text squished
      })
    })
    describe(".top", () => {
      beforeEach(function() {
        const dataConnector = { getFields, query: _ => _ }
        return cf.crossfilter(dataConnector, "users").then(crsfltr => {
          crossfilter = crsfltr
          dimension = crossfilter.dimension("id")
          dimension.projectOnAllDimensions(true)
        })
      })
      it("returns empty object if no query", () => {
        dimension.projectOnAllDimensions(false)
        expect(dimension.top()).to.eql({})
        dimension.top(undefined, undefined, null, function(error, result) {
          expect(result).to.eql({})
        })
      })
      it("constructs and runs query", () => {
        expect(dimension.top(1, 2)).to.eq(
          "SELECT id FROM users ORDER BY id DESC LIMIT 1 OFFSET 2"
        )
      })
      it("orders by orderExpression if any", () => {
        dimension.order("custom")
        expect(dimension.top()).to.include("ORDER BY custom")
      })
      it("orders by dimensionExpression if no orderExpression", () => {
        expect(dimension.top()).to.include("ORDER BY id")
      })
      it("limits results if k < Infinity", () => {
        expect(dimension.top(10)).to.include("LIMIT 10")
      })
      it("does not limit result if k is Infinity", () => {
        expect(dimension.top(Infinity)).to.not.include("LIMIT")
      })
      it("can offset query", () => {
        expect(dimension.top(10, 20)).to.include("OFFSET 20")
      })
      it("can return sync results", function() {
        const dataConnector = { getFields, query: _ => 2 }
        return cf.crossfilter(dataConnector, "users").then(crsfltr => {
          crossfilter = crsfltr
          dimension = crossfilter.dimension("id")
          dimension.projectOnAllDimensions(true)
          expect(dimension.top(1)).to.eq(2)
        })
      })
      xit("can return async results", () => {
        expect(callback).to.work
      })
      it("can include a dimension's rowid", () => {
        expect(dimension.top(Infinity, 1, true)).to.eq(
          "SELECT id,users.rowid FROM users ORDER BY id DESC OFFSET 1"
        ) // TODO rowid squished
      })
      it("can use a samplingRatio < 1 with filterQuery", () => {
        dimension.samplingRatio(0.5)
        expect(dimension.top(Infinity)).to.eq(
          "SELECT id FROM users WHERE  MOD(users.rowid * 265445761, 4294967296) < 2147483648 ORDER BY id DESC"
        ) // TODO extra space between WHERE & MOD
      })
      it("can use a samplingRatio < 1 without filterQuery", () => {
        dimension.filterExact(1)
        dimension.samplingRatio(0.5)
        expect(dimension.top(Infinity)).to.eq(
          "SELECT id FROM users WHERE id = 1 AND  MOD(users.rowid * 265445761, 4294967296) < 2147483648 ORDER BY id DESC"
        ) // TODO extra space between AND & MOD
      })
      it("can use a join statement with no filterQuery or samplingRatio", () => {
        const dataConnector = { getFields }
        const dataTables = "tableA"
        const joinAttrs = [
          { table1: "table1", table2: "table2", attr1: "id", attr2: "x_id" }
        ]
        crossfilter.setDataAsync(dataConnector, dataTables, joinAttrs)
        dimension.samplingRatio(0.5)
        expect(dimension.top(Infinity)).to.eq(
          "SELECT id FROM tableA WHERE  MOD(tableA.rowid * 265445761, 4294967296) < 2147483648 AND table1.id = table2.x_id ORDER BY id DESC"
        ) // TODO extra space between WHERE & MOD
      })
      it("can use a joinStatement with no filterQuery and samplingRatio >= 1", () => {
        const dataConnector = { getFields }
        const dataTables = "tableA"
        const joinAttrs = [
          { table1: "table1", table2: "table2", attr1: "id", attr2: "x_id" }
        ]
        crossfilter.setDataAsync(dataConnector, dataTables, joinAttrs)
        dimension.samplingRatio(1.5)
        expect(dimension.top(Infinity)).to.eq(
          "SELECT id FROM tableA WHERE table1.id = table2.x_id ORDER BY id DESC"
        ) // TODO extra space between WHERE & MOD
      })
      it("AND concats multiple filters/dimensions", () => {
        dimension.filter([1, 2])
        dimension = crossfilter.dimension(["rx", "sex"])
        dimension.filter([3, 4])
        dimension.projectOnAllDimensions(true)
        expect(dimension.top(Infinity)).to.eq(
          "SELECT id,rx, sex FROM users WHERE (id >= 1 AND id <= 2) AND rx = 3 AND sex = 4 ORDER BY rx, sex DESC"
        ) // TODO text squished
      })
    })

    describe(".writeBottomQuery", () => {
      beforeEach(function() {
        const dataConnector = { getFields, query: _ => _ }
        return cf.crossfilter(dataConnector, "users").then(crsfltr => {
          crossfilter = crsfltr
          dimension = crossfilter.dimension("id")
          dimension.projectOnAllDimensions(true)
        })
      })
      it("returns empty object if no query", () => {
        dimension.projectOnAllDimensions(false)
        expect(dimension.writeBottomQuery()).to.eql("")
      })
      it("constructs query", () => {
        expect(dimension.writeBottomQuery(1, 2)).to.eq(
          "SELECT id FROM users ORDER BY id ASC LIMIT 1 OFFSET 2"
        )
      })
      it("orders by orderExpression if any", () => {
        dimension.order("custom")
        expect(dimension.writeBottomQuery()).to.include("ORDER BY custom")
      })
      it("orders by dimensionExpression if no orderExpression", () => {
        expect(dimension.writeBottomQuery()).to.include("ORDER BY id")
      })
      it("limits results if k < Infinity", () => {
        expect(dimension.writeBottomQuery(10)).to.include("LIMIT 10")
      })
      it("does not limit result if k is Infinity", () => {
        expect(dimension.writeBottomQuery(Infinity)).to.not.include("LIMIT")
      })
      it("can offset query", () => {
        expect(dimension.writeBottomQuery(10, 20)).to.include("OFFSET 20")
      })
    })
    describe(".bottom", () => {
      beforeEach(function() {
        const dataConnector = { getFields, query: _ => _ }
        return cf.crossfilter(dataConnector, "users").then(crsfltr => {
          crossfilter = crsfltr
          dimension = crossfilter.dimension("id")
          dimension.projectOnAllDimensions(true)
        })
      })
      it("returns empty object if no query", () => {
        dimension.projectOnAllDimensions(false)
        expect(dimension.bottom()).to.eql({})
        dimension.bottom(undefined, undefined, null, function(error, result) {
          expect(result).to.eql({})
        })
      })
      it("constructs and runs query", () => {
        expect(dimension.bottom(1, 2)).to.eq(
          "SELECT id FROM users ORDER BY id ASC LIMIT 1 OFFSET 2"
        )
      })
      it("orders by orderExpression if any", () => {
        dimension.order("custom")
        expect(dimension.bottom()).to.include("ORDER BY custom")
      })
      it("orders by dimensionExpression if no orderExpression", () => {
        expect(dimension.bottom()).to.include("ORDER BY id")
      })
      it("limits results if k < Infinity", () => {
        expect(dimension.bottom(10)).to.include("LIMIT 10")
      })
      it("does not limit result if k is Infinity", () => {
        expect(dimension.bottom(Infinity)).to.not.include("LIMIT")
      })
      it("can offset query", () => {
        expect(dimension.bottom(10, 20)).to.include("OFFSET 20")
      })
      it("can return sync results", function() {
        const dataConnector = { getFields, query: _ => 2 }
        return cf.crossfilter(dataConnector, "users").then(crsfltr => {
          crossfilter = crsfltr
          dimension = crossfilter.dimension("id")
          dimension.projectOnAllDimensions(true)
          expect(dimension.bottom(1)).to.eq(2)
        })
      })
      xit("can return async results", () => {
        expect(callback).to.work
      })
    })
    describe(".bottomAsync", () => {})
    describe(".group", () => {
      let group
      beforeEach(() => {
        crossfilter.setDataAsync({ query: _ => _, getFields }, "table1")
        group = dimension.group()
      })
      xit("returns group object", () => {
        expect(dimension.group()).to.eql(group)
      })
      it("returns itself", () => {
        expect(group.type).to.eq("group")
      })
      describe(".getCrossfilter", () => {
        it("returns parent crossfilter", () => {
          expect(group.getCrossfilter()).to.eq(crossfilter)
        })
      })
      describe(".getCrossfilterId", () => {
        it("returns parent crossfilter's id", () => {
          expect(group.getCrossfilterId()).to.eq(crossfilter.getId())
        })
      })
      describe(".getTable", () => {
        it("returns table", () => {
          expect(group.getTable()).to.eql(crossfilter.getTable())
        })
      })
      describe(".reduceCount", () => {
        // TODO duplicates crossfilter.groupAll methods
        it("returns own group object", () => {
          expect(group.reduceCount()).to.eql(group)
        })
        it("sets reduceExpression to count(*) with no arg", () => {
          group.reduceCount()
          expect(group.getReduceExpression()).to.eq("COUNT(*) AS val")
        })
        it("sets reduceExpression to based on arg", () => {
          group.reduceCount("x.id", "idcnt")
          expect(group.getReduceExpression()).to.eq("COUNT(x.id) AS idcnt")
        })
      })
      describe(".reduceSum", () => {
        // TODO duplicates crossfilter.groupAll methods
        it("returns own group object", () => {
          expect(group.reduceSum()).to.eql(group)
        })
        xit("validates input")
        it("sets reduceExpression with no arg", () => {
          group.reduceSum("x.id")
          expect(group.getReduceExpression()).to.eq("SUM(x.id) AS val")
        })
        it("sets reduceExpression with arg", () => {
          group.reduceSum("x.id", "idsum")
          expect(group.getReduceExpression()).to.eq("SUM(x.id) AS idsum")
        })
      })
      describe(".reduceAvg", () => {
        // TODO duplicates crossfilter.groupAll methods
        it("returns own group object", () => {
          expect(group.reduceAvg()).to.eql(group)
        })
        xit("validates input")
        it("sets reduceExpression with no arg", () => {
          group.reduceAvg("x.id")
          expect(group.getReduceExpression()).to.eq("AVG(x.id) AS val")
        })
        it("sets reduceExpression with arg", () => {
          group.reduceAvg("x.id", "idavg")
          expect(group.getReduceExpression()).to.eq("AVG(x.id) AS idavg")
        })
      })
      describe(".reduceMin", () => {
        // TODO duplicates crossfilter.groupAll methods
        it("returns own group object", () => {
          expect(group.reduceMin()).to.eql(group)
        })
        xit("validates input")
        it("sets reduceExpression with no arg", () => {
          group.reduceMin("x.id")
          expect(group.getReduceExpression()).to.eq("MIN(x.id) AS val")
        })
        it("sets reduceExpression with arg", () => {
          group.reduceMin("x.id", "idmin")
          expect(group.getReduceExpression()).to.eq("MIN(x.id) AS idmin")
        })
      })
      describe(".reduceMax", () => {
        // TODO duplicates crossfilter.groupAll methods
        it("returns own group object", () => {
          expect(group.reduceMax()).to.eql(group)
        })
        xit("validates input")
        it("sets reduceExpression with no arg", () => {
          group.reduceMax("x.id")
          expect(group.getReduceExpression()).to.eq("MAX(x.id) AS val")
        })
        it("sets reduceExpression with arg", () => {
          group.reduceMax("x.id", "idmax")
          expect(group.getReduceExpression()).to.eq("MAX(x.id) AS idmax")
        })
      })
      describe(".reduce", () => {
        // TODO duplicates crossfilter.groupAll methods
        it("returns own group object", () => {
          expect(group.reduce([])).to.eql(group)
        })
        xit("validates input")
        it("sets reduceExpression", () => {
          const expressions = [
            { expression: "x.id", agg_mode: "MIN", name: "minx" }
          ]
          group.reduce(expressions)
          expect(group.getReduceExpression()).to.eq("MIN(x.id) AS minx")
        })
        it("sets reduceExpression when agg_mode is COUNT", () => {
          const expressions = [
            { expression: "x.id", agg_mode: "COUNT", name: "minx" }
          ]
          group.reduce(expressions)
          expect(group.getReduceExpression()).to.eq("COUNT(x.id) AS minx")
        })
        it("sets reduceExpression when agg_mode is COUNT and no expression", () => {
          const expressions = [{ agg_mode: "COUNT", name: "minx" }]
          group.reduce(expressions)
          expect(group.getReduceExpression()).to.eq("COUNT(*) AS minx")
        })
        it("sets reduceExpression for multiple expressions", () => {
          const expressions = [
            { expression: "x.id", agg_mode: "AVG", name: "avgx" },
            { expression: "y.rx", agg_mode: "MAX", name: "maxy" }
          ]
          group.reduce(expressions)
          expect(group.getReduceExpression()).to.eq(
            "AVG(x.id) AS avgx,MAX(y.rx) AS maxy"
          )
        })
        it("doesn't null out * when used in agg function", () => {
          const expressions = [
            { agg_mode: "COUNT", name: "count_all" },
            { expression: "*", agg_mode: "COUNT", name: "count_star" }
          ]
          group.reduce(expressions)
          expect(group.top(1)).to.eq(
            "SELECT bargle as key0,COUNT(*) AS count_all,COUNT(*) AS count_star FROM table1 GROUP BY key0 ORDER BY count_all DESC,count_star DESC LIMIT 1"
          )
        })
        it("sets composite reduceExpression", () => {
          const expressions = [
            {
              expression: "arrdelay+depdelay",
              agg_mode: "AVG",
              name: "avg_delays",
              isComposite: true
            }
          ]
          group.reduce(expressions)
          expect(group.top(1)).to.eq(
            "SELECT bargle as key0,AVG(arrdelay+depdelay) AS avg_delays FROM table1 GROUP BY key0 ORDER BY avg_delays DESC LIMIT 1"
          )
        })
      })
      describe(".reduceMulti", () => {
        // TODO duplicates crossfilter.groupAll methods
        it("is an alias for .reduce", () => {
          expect(group.reduce).to.eq(group.reduceMulti)
        })
      })
      describe(".size", () => {
        // TODO similar to crossfilter.size
        it("returns number of records", () => {
          crossfilter.setDataAsync(
            { getFields: () => [], query: () => [{ n: 123 }] },
            null,
            []
          )
          expect(group.size()).to.eql(123)
        })
        xit("constructs a valid query when _joinStmt is undefined")
      })
      describe(".writeTopQuery", () => {
        beforeEach(function() {
          const dataConnector = { getFields, query: _ => _ }
          return cf.crossfilter(dataConnector, "users").then(crsfltr => {
            crossfilter = crsfltr
            dimension = crossfilter.dimension("id")
            group = dimension.group()
            dimension.projectOnAllDimensions(true)
          })
        })
        it("constructs query", () => {
          expect(group.writeTopQuery(1, 2)).to.eq(
            "SELECT id as key0,COUNT(*) AS val FROM users GROUP BY key0 ORDER BY val DESC LIMIT 1 OFFSET 2"
          )
        })
        it("orders by orderExpression if any", () => {
          group.order("custom")
          expect(group.writeTopQuery()).to.include("ORDER BY custom")
        })
        it("orders by groupExpression if no orderExpression", () => {
          expect(group.writeTopQuery()).to.include("ORDER BY val")
        })
        it("limits results if k < Infinity", () => {
          expect(group.writeTopQuery(10)).to.include("LIMIT 10")
        })
        it("does not limit result if k is Infinity", () => {
          expect(group.writeTopQuery(Infinity)).to.not.include("LIMIT")
        })
        it("can offset query", () => {
          expect(group.writeTopQuery(10, 20)).to.include("OFFSET 20")
        })
        it("works with reduce expressions", () => {
          group.reduce([
            { expression: "age", agg_mode: "MAX", name: "max_age" },
            { expression: "lbs", agg_mode: "AVG", name: "avg_lbs" },
            { expression: "cty", agg_mode: "COUNT", name: "cnt_cty" }
          ])
          expect(group.writeTopQuery(1)).to.eq(
            "SELECT id as key0,MAX(age) AS max_age,AVG(lbs) AS avg_lbs,COUNT(cty) AS cnt_cty FROM users WHERE age IS NOT NULL AND lbs IS NOT NULL AND cty IS NOT NULL GROUP BY key0 ORDER BY max_age DESC,avg_lbs DESC,cnt_cty DESC LIMIT 1"
          )
        })
        it("works with reduce expressions including COUNT(*)", () => {
          group.reduce([
            { expression: "lbs", agg_mode: "AVG", name: "avg_lbs" },
            { agg_mode: "COUNT", name: "cnt_cty" },
            { expression: "*", agg_mode: "COUNT", name: "cnt_bad" }
          ])
          expect(group.writeTopQuery(1)).to.eq(
            "SELECT id as key0,AVG(lbs) AS avg_lbs,COUNT(*) AS cnt_cty,COUNT(*) AS cnt_bad FROM users WHERE lbs IS NOT NULL GROUP BY key0 ORDER BY avg_lbs DESC,cnt_cty DESC,cnt_bad DESC LIMIT 1"
          )
        })
        it("appropriately handles render queries with rowid dimension", () => {
          var dim = crossfilter.dimension(["bargle", "rowid"])
          var group = dim.group()
          expect(group.writeTopQuery(1, 2)).to.eql(
            "SELECT bargle as key0,rowid as key1,COUNT(*) AS val FROM users GROUP BY key0, key1 ORDER BY val DESC LIMIT 1 OFFSET 2"
          )
          expect(group.writeTopQuery(1, 2, false, true)).to.eql(
            "SELECT bargle as key0,rowid,COUNT(*) AS val FROM users GROUP BY key0, rowid ORDER BY val DESC LIMIT 1 OFFSET 2"
          )
        })
      })

      describe(".top", () => {
        beforeEach(function() {
          const dataConnector = { getFields, query: _ => _ }
          return cf.crossfilter(dataConnector, "users").then(crsfltr => {
            crossfilter = crsfltr
            dimension = crossfilter.dimension("id")
            group = dimension.group()
            dimension.projectOnAllDimensions(true)
          })
        })
        it("constructs and runs query", () => {
          expect(group.top(1, 2)).to.eq(
            "SELECT id as key0,COUNT(*) AS val FROM users GROUP BY key0 ORDER BY val DESC LIMIT 1 OFFSET 2"
          )
        })
        it("orders by orderExpression if any", () => {
          group.order("custom")
          expect(group.top()).to.include("ORDER BY custom")
        })
        it("orders by groupExpression if no orderExpression", () => {
          expect(group.top()).to.include("ORDER BY val")
        })
        it("limits results if k < Infinity", () => {
          expect(group.top(10)).to.include("LIMIT 10")
        })
        it("does not limit result if k is Infinity", () => {
          expect(group.top(Infinity)).to.not.include("LIMIT")
        })
        it("can offset query", () => {
          expect(group.top(10, 20)).to.include("OFFSET 20")
        })
        it("works with reduce expressions", () => {
          group.reduce([
            { expression: "age", agg_mode: "MAX", name: "max_age" },
            { expression: "lbs", agg_mode: "AVG", name: "avg_lbs" },
            { expression: "cty", agg_mode: "COUNT", name: "cnt_cty" }
          ])
          expect(group.top(1)).to.eq(
            "SELECT id as key0,MAX(age) AS max_age,AVG(lbs) AS avg_lbs,COUNT(cty) AS cnt_cty FROM users WHERE age IS NOT NULL AND lbs IS NOT NULL AND cty IS NOT NULL GROUP BY key0 ORDER BY max_age DESC,avg_lbs DESC,cnt_cty DESC LIMIT 1"
          )
        })
        it("works with reduce expressions including COUNT(*)", () => {
          group.reduce([
            { expression: "lbs", agg_mode: "AVG", name: "avg_lbs" },
            { agg_mode: "COUNT", name: "cnt_cty" },
            { expression: "*", agg_mode: "COUNT", name: "cnt_bad" }
          ])
          expect(group.top(1)).to.eq(
            "SELECT id as key0,AVG(lbs) AS avg_lbs,COUNT(*) AS cnt_cty,COUNT(*) AS cnt_bad FROM users WHERE lbs IS NOT NULL GROUP BY key0 ORDER BY avg_lbs DESC,cnt_cty DESC,cnt_bad DESC LIMIT 1"
          )
        })
        it("can return sync results", function() {
          const dataConnector = { getFields, query: _ => 2 }
          return cf.crossfilter(dataConnector, "users").then(crsfltr => {
            crossfilter = crsfltr
            dimension = crossfilter.dimension("id")
            dimension.projectOnAllDimensions(true)
            group = dimension.group()
            expect(group.top(1)).to.eq(2)
          })
        })
        xit("can return async results", () => {
          expect(callback).to.work
        })
      })
      describe(".topAsync", () => {
        let connector
        beforeEach(function() {
          getFieldsReturnValue = [
            {
              name: "contrib_date",
              type: "DATE",
              is_array: false,
              is_dict: false
            },
            {
              name: "event_date",
              type: "DATE",
              is_array: false,
              is_dict: false
            }
          ]
          connector = {
            platform: () => "mapd",
            getFields,
            query: chai.spy((a, b, cb) => {
              return Promise.resolve(cb(null, []))
            })
          }
          return cf.crossfilter(connector, "contributions").then(crsfltr => {
            crossfilter = crsfltr
            dimension = crossfilter.dimension(["contrib_date", "event_date"])
            dimension.projectOnAllDimensions(true)
          })
        })
        it("should apply the proper binParams to the query", function() {
          return dimension
            .group()
            .binParams([
              {
                binBounds: [new Date("1/1/2006"), new Date("1/1/2007")],
                numBins: 400,
                timeBin: "month"
              },
              {
                binBounds: [new Date("1/1/2006"), new Date("1/1/2007")],
                numBins: 400,
                timeBin: "month"
              }
            ])
            .topAsync(20, 20, null)
            .then(result => {
              if (isPST) {
                expect(connector.query).to.have.been.called.with(
                  "SELECT date_trunc(month, CAST(contrib_date AS TIMESTAMP(0))) as key0,date_trunc(month, CAST(event_date AS TIMESTAMP(0))) as key1,COUNT(*) AS val FROM contributions WHERE (CAST(contrib_date AS TIMESTAMP(0)) >= TIMESTAMP(0) '2006-01-01 08:00:00' AND CAST(contrib_date AS TIMESTAMP(0)) <= TIMESTAMP(0) '2007-01-01 08:00:00') AND (CAST(event_date AS TIMESTAMP(0)) >= TIMESTAMP(0) '2006-01-01 08:00:00' AND CAST(event_date AS TIMESTAMP(0)) <= TIMESTAMP(0) '2007-01-01 08:00:00') GROUP BY key0, key1 ORDER BY val DESC LIMIT 20 OFFSET 20"
                )
              } else {
                expect(connector.query).to.have.been.called.with(
                  "SELECT date_trunc(month, CAST(contrib_date AS TIMESTAMP(0))) as key0,date_trunc(month, CAST(event_date AS TIMESTAMP(0))) as key1,COUNT(*) AS val FROM contributions WHERE (CAST(contrib_date AS TIMESTAMP(0)) >= TIMESTAMP(0) '2006-01-01 00:00:00' AND CAST(contrib_date AS TIMESTAMP(0)) <= TIMESTAMP(0) '2007-01-01 00:00:00') AND (CAST(event_date AS TIMESTAMP(0)) >= TIMESTAMP(0) '2006-01-01 00:00:00' AND CAST(event_date AS TIMESTAMP(0)) <= TIMESTAMP(0) '2007-01-01 00:00:00') GROUP BY key0, key1 ORDER BY val DESC LIMIT 20 OFFSET 20"
                )
              }
            })
        })
        it("should apply the proper binParams to the query when using extract", function() {
          return dimension
            .group()
            .binParams([
              {
                binBounds: [new Date("1/1/2006"), new Date("1/1/2007")],
                numBins: 400,
                timeBin: "month",
                extract: true
              },
              {
                binBounds: [new Date("1/1/2006"), new Date("1/1/2007")],
                numBins: 400,
                timeBin: "month"
              }
            ])
            .topAsync(20, 20, null)
            .then(result => {
              if (isPST) {
                expect(connector.query).to.have.been.called.with(
                  "SELECT extract(month from contrib_date) as key0,date_trunc(month, CAST(event_date AS TIMESTAMP(0))) as key1,COUNT(*) AS val FROM contributions WHERE (CAST(event_date AS TIMESTAMP(0)) >= TIMESTAMP(0) '2006-01-01 08:00:00' AND CAST(event_date AS TIMESTAMP(0)) <= TIMESTAMP(0) '2007-01-01 08:00:00') GROUP BY key0, key1 ORDER BY val DESC LIMIT 20 OFFSET 20"
                )
              } else {
                expect(connector.query).to.have.been.called.with(
                  "SELECT extract(month from contrib_date) as key0,date_trunc(month, CAST(event_date AS TIMESTAMP(0))) as key1,COUNT(*) AS val FROM contributions WHERE (CAST(event_date AS TIMESTAMP(0)) >= TIMESTAMP(0) '2006-01-01 00:00:00' AND CAST(event_date AS TIMESTAMP(0)) <= TIMESTAMP(0) '2007-01-01 00:00:00') GROUP BY key0, key1 ORDER BY val DESC LIMIT 20 OFFSET 20"
                )
              }
            })
        })
        it("should handle error case", function() {
          const error = "ERROR"
          connector = {
            platform: () => "mapd",
            getFields,
            query: chai.spy((a, b, cb) => Promise.reject(cb(error)))
          }
          return cf.crossfilter(connector, "contributions").then(crsfltr => {
            crossfilter = crsfltr
            dimension = crossfilter.dimension(["contrib_date"])
            return dimension
              .projectOnAllDimensions(true)
              .group()
              .topAsync()
              .catch(e => {
                expect(e).to.equal(error)
              })
          })
        })
      })
      describe(".writeBottomQuery", () => {
        beforeEach(function() {
          const dataConnector = { getFields, query: _ => _ }
          return cf.crossfilter(dataConnector, "users").then(crsfltr => {
            crossfilter = crsfltr
            dimension = crossfilter.dimension("id")
            group = dimension.group()
            dimension.projectOnAllDimensions(true)
          })
        })
        it("constructs query", () => {
          expect(group.writeBottomQuery(1, 2)).to.eq(
            "SELECT id as key0,COUNT(*) AS val FROM users GROUP BY key0 ORDER BY val LIMIT 1 OFFSET 2"
          )
        })
        it("orders by orderExpression if any", () => {
          group.order("custom")
          expect(group.writeBottomQuery()).to.include("ORDER BY custom")
        })
        it("orders by groupExpression if no orderExpression", () => {
          expect(group.writeBottomQuery()).to.include("ORDER BY val")
        })
        it("limits results if k < Infinity", () => {
          expect(group.writeBottomQuery(10)).to.include("LIMIT 10")
        })
        it("does not limit result if k is Infinity", () => {
          expect(group.writeBottomQuery(Infinity)).to.not.include("LIMIT")
        })
        it("can offset query", () => {
          expect(group.writeBottomQuery(10, 20)).to.include("OFFSET 20")
        })
        it("works with reduce expressions", () => {
          group.reduce([
            { expression: "id", agg_mode: "MIN", name: "min_id" },
            { expression: "rx", agg_mode: "SUM", name: "sum_rx" }
          ])
          expect(group.writeBottomQuery(1)).to.eq(
            "SELECT id as key0,MIN(id) AS min_id,SUM(rx) AS sum_rx FROM users WHERE id IS NOT NULL AND rx IS NOT NULL GROUP BY key0 ORDER BY min_id,sum_rx LIMIT 1"
          )
        })
        it("appropriately handles render queries with rowid dimension", () => {
          var dim = crossfilter.dimension(["bargle", "rowid"])
          var group = dim.group()
          expect(group.writeBottomQuery(1, 2)).to.eql(
            "SELECT bargle as key0,rowid as key1,COUNT(*) AS val FROM users GROUP BY key0, key1 ORDER BY val LIMIT 1 OFFSET 2"
          )
          expect(group.writeBottomQuery(1, 2, false, true)).to.eql(
            "SELECT bargle as key0,rowid,COUNT(*) AS val FROM users GROUP BY key0, rowid ORDER BY val LIMIT 1 OFFSET 2"
          )
        })
      })
      describe(".bottom", () => {
        beforeEach(function() {
          const dataConnector = { getFields, query: _ => _ }
          return cf.crossfilter(dataConnector, "users").then(crsfltr => {
            crossfilter = crsfltr
            dimension = crossfilter.dimension("id")
            group = dimension.group()
            dimension.projectOnAllDimensions(true)
          })
        })
        it("constructs and runs query", () => {
          expect(group.bottom(1, 2)).to.eq(
            "SELECT id as key0,COUNT(*) AS val FROM users GROUP BY key0 ORDER BY val LIMIT 1 OFFSET 2"
          )
        })
        it("orders by orderExpression if any", () => {
          group.order("custom")
          expect(group.bottom()).to.include("ORDER BY custom")
        })
        it("orders by groupExpression if no orderExpression", () => {
          expect(group.bottom()).to.include("ORDER BY val")
        })
        it("limits results if k < Infinity", () => {
          expect(group.bottom(10)).to.include("LIMIT 10")
        })
        it("does not limit result if k is Infinity", () => {
          expect(group.bottom(Infinity)).to.not.include("LIMIT")
        })
        it("can offset query", () => {
          expect(group.bottom(10, 20)).to.include("OFFSET 20")
        })
        it("works with reduce expressions", () => {
          group.reduce([
            { expression: "id", agg_mode: "MIN", name: "min_id" },
            { expression: "rx", agg_mode: "SUM", name: "sum_rx" }
          ])
          expect(group.bottom(1)).to.eq(
            "SELECT id as key0,MIN(id) AS min_id,SUM(rx) AS sum_rx FROM users WHERE id IS NOT NULL AND rx IS NOT NULL GROUP BY key0 ORDER BY min_id,sum_rx LIMIT 1"
          )
        })
        it("can return sync results", function() {
          const dataConnector = { getFields, query: _ => 2 }
          return cf.crossfilter(dataConnector, "users").then(crsfltr => {
            crossfilter = crsfltr
            dimension = crossfilter.dimension("id")
            dimension.projectOnAllDimensions(true)
            group = dimension.group()
            expect(group.bottom(1)).to.eq(2)
          })
        })
        xit("can return async results", () => {
          expect(callback).to.work
        })
      })
      describe(".bottomAsync", () => {
        // TODO duplicates dimension methods
        let connector
        beforeEach(function() {
          getFieldsReturnValue = [
            {
              name: "contrib_date",
              type: "DATE",
              is_array: false,
              is_dict: false
            }
          ]
          connector = {
            platform: () => "mapd",
            getFields,
            query: chai.spy((a, b, cb) => Promise.resolve(cb(null, [])))
          }
          return cf.crossfilter(connector, "contributions").then(crsfltr => {
            crossfilter = crsfltr
            dimension = crossfilter.dimension("contrib_date")
            dimension.projectOnAllDimensions(true)
          })
        })
      })
      describe(".order", () => {
        it("returns own group object", () => {
          expect(group.order()).to.eq(group)
        })
        it("sets orderExpression", () => {
          group.order("created_at")
          expect(group.top(Infinity)).to.include("ORDER BY created_at DESC")
        })
      })
      describe(".orderNatural", () => {
        it("returns own group object", () => {
          expect(group.orderNatural()).to.eq(group)
        })
        it("nulls out orderExpression", () => {
          group.order("created_at")
          expect(group.top(Infinity)).to.include("ORDER BY created_at DESC")
          group.orderNatural()
          expect(group.top(Infinity)).to.include("ORDER BY val DESC")
        })
      })

      describe(".getProjectOn", () => {
        it("returns dimension and default count", () => {
          expect(group.getProjectOn()).to.eql([
            "bargle as key0",
            "COUNT(*) AS val"
          ])
        })
        it("handles rowid properly for render requests", () => {
          var dim = crossfilter.dimension(["bargle", "rowid"])
          var group = dim.group()
          expect(group.getProjectOn()).to.eql([
            "bargle as key0",
            "rowid as key1",
            "COUNT(*) AS val"
          ])
          expect(group.getProjectOn(true)).to.eql([
            "bargle as key0",
            "rowid",
            "COUNT(*) AS val"
          ])
        })
        it("handles timed bins", () => {
          group.binParams([
            {
              binBounds: [
                new Date("Sat Dec 31 1988 16:00:00 GMT-0800 (PST)"),
                new Date("Wed Oct 14 2015 17:00:00 GMT-0700 (PDT)")
              ],
              numBins: 400,
              extract: false,
              timeBin: "month"
            }
          ])

          expect(group.getProjectOn()).to.eql([
            "date_trunc(month, bargle) as key0",
            "COUNT(*) AS val"
          ])

          group.binParams([
            {
              binBounds: [
                new Date("Sat Dec 31 1988 16:00:00 GMT-0800 (PST)"),
                new Date("Wed Oct 14 2015 17:00:00 GMT-0700 (PDT)")
              ],
              numBins: 400,
              extract: true,
              timeBin: "isodow"
            }
          ])

          expect(group.getProjectOn()).to.eql([
            "extract(isodow from bargle) as key0",
            "COUNT(*) AS val"
          ])
        })

        it("call getProjectOn internally using bins", () => {
          group.binParams([
            {
              binBounds: [
                new Date("Sat Dec 31 1988 16:00:00 GMT-0800 (PST)"),
                new Date("Wed Oct 14 2015 17:00:00 GMT-0700 (PDT)")
              ],
              numBins: 400,
              extract: false,
              timeBin: "month"
            }
          ])
          expect(group.getProjectOn(false, group.binParams())).to.eql([
            "date_trunc(month, bargle) as key0",
            "COUNT(*) AS val"
          ])

          group.binParams([
            {
              binBounds: [
                new Date("Sat Dec 31 1988 16:00:00 GMT-0800 (PST)"),
                new Date("Wed Oct 14 2015 17:00:00 GMT-0700 (PDT)")
              ],
              numBins: 400,
              extract: true,
              timeBin: "isodow"
            }
          ])

          expect(group.getProjectOn(false, group.binParams())).to.eql([
            "extract(isodow from bargle) as key0",
            "COUNT(*) AS val"
          ])
        })

        it("handles date bin bounds with undefined timed bin", () => {
          // TODO(croot): This was written to satisfy coverage requirements.
          // I do not know if the result here is expected, or if it's a reasonable
          // test. I did not write the code that generates the 'cast((extract(epoch from ...)) ...)'
          // code

          const binParams = {
            binBounds: [
              new Date("Sat Dec 31 1988 16:00:00 GMT-0800 (PST)"),
              new Date("Wed Oct 14 2015 17:00:00 GMT-0700 (PDT)")
            ],
            numBins: 400,
            extract: false,
            timeBin: ""
          }
          group.binParams([binParams])
          const filterRange =
            (binParams.binBounds[1].getTime() -
              binParams.binBounds[0].getTime()) *
            0.001
          const binsPerUnit = binParams.numBins / filterRange

          expect(group.getProjectOn(false, group.binParams())).to.eql([
            `cast((extract(epoch from bargle) - 599616000) * ${binsPerUnit} as int) as key0`,
            "COUNT(*) AS val"
          ])
          expect(group.getProjectOn()).to.eql([
            `cast((extract(epoch from bargle) - 599616000) * ${binsPerUnit} as int) as key0`,
            "COUNT(*) AS val"
          ])
        })

        it("handles non time bins", () => {
          // TODO(croot): This was written to satisfy coverage requirements.
          // I do not know if the result here is expected, or if it's a reasonable
          // test. I did not write the code that generates the 'cast((dim - 1) ...)'
          // code
          // NOTE: as of 11/16/2016 group.binParams() does not accept
          // non-time bins. I do not no if this is an oversight or intentional.
          // There is code in getBinnedDimExpression to deal with non-time
          // bins, so that's why this test is here, but it's not available
          // to the outside world right now, only if group.getProjectOn() were
          // called internally, but even then, the code this is testing wouldn't
          // be reached. So this is ultimately testing code that is unreachable
          // currently
          const queryBinParams = [{ binBounds: [1, 2] }]
          expect(group.getProjectOn(false, queryBinParams)).to.eql([
            "cast((cast(bargle as float) - 1) * 0 as int) as key0",
            "COUNT(*) AS val"
          ])

          const queryBinNumParams = [{ binBounds: [1, 2], numBins: 400 }]
          expect(group.getProjectOn(false, queryBinNumParams)).to.eql([
            "cast((cast(bargle as float) - 1) * 400 as int) as key0",
            "COUNT(*) AS val"
          ])
        })

        it("test UNNEST", () => {
          // TODO(croot): This was written to satisfy coverage requirements.
          // I do not know if the result here is expected, or if it's a reasonable
          // test. I did not write the code that generates the 'UNNEST(...)'
          // code
          const columnsArray = [
            {
              name: "arraycolumn",
              type: "double",
              is_array: true,
              is_dict: false
            }
          ]
          getFieldsReturnValue = columnsArray
          const dataConnector = { getFields }
          return crossfilter.setDataAsync(dataConnector, "tableA").then(() => {
            dimension = crossfilter.dimension("arraycolumn")
            group = dimension.group()
            expect(group.getProjectOn()).to.eql([
              "UNNEST(arraycolumn) as key0",
              "COUNT(*) AS val"
            ])
          })
        })
      })

      describe(".binParams", () => {
        it("returns own group object", () => {
          expect(group.binParams([])).to.eql(group)
        })
        it("returns bin params if no args", () => {
          expect(group.binParams()).to.eql([])
          const min = new Date()
          const max = new Date()
          group.binParams([{ binBounds: [min, max] }])
          expect(group.binParams()).to.eql([{ binBounds: [min, max] }])
        })

        it("should handle float binBounds and format floats to 10 digits", () => {
          group.binParams([
            {
              binBounds: [100.202002, 600.01231],
              numBins: 400,
              timeBin: false
            }
          ])
          expect(group.binParams()[0].binBounds[1]).to.equal(600.01231)
        })
      })
      describe(".setBinParams", () => {
        it("is alias for binParams", () => {
          expect(group.setBinParams).to.eq(group.binParams)
        })
      })
      describe(".setTargetSlot", () => {
        it("sets targetSlot", () => {
          expect(group.getTargetSlot()).to.eq(0)
          group.setTargetSlot(2)
          expect(group.getTargetSlot()).to.eq(2)
        })
      })
      describe(".getTargetSlot", () => {
        it("returns targetSlot", () => {
          expect(group.getTargetSlot()).to.eq(0)
          group.setTargetSlot(2)
          expect(group.getTargetSlot()).to.eq(2)
        })
      })
      xdescribe(".setBoundByFilter", () => {
        it("returns own group object", () => {
          expect(group.setBoundByFilter()).to.eql(group)
        })
        xit("sets boundByFilter", () => {
          group.setBoundByFilter("idk")
          // assigns to binBounds
        })
      })
      describe(".allAsync", () => {
        it("is alias for .all", () => {
          expect(group.allAsync).to.eq(group.all)
        })
      })
      xdescribe(".truncDate", () => {
        // TODO blows up always
        it("returns own group object", () => {
          expect(group.truncDate()).to.eq(group)
        })
        xit("sets dateTruncLevel to arg")
        xit("sets binCount to binCountIn")
      })
      describe(".all", () => {
        it("returns sync query result", () => {
          crossfilter.setDataAsync({ query: _ => 123, getFields: () => [] })
          group = dimension.group()
          expect(group.all()).to.eq(123)
        })
        it("creates query", () => {
          expect(group.all()).to.eq(
            "SELECT bargle as key0,COUNT(*) AS val FROM table1 GROUP BY key0 ORDER BY key0"
          ) // TODO count squished
        })
        it("handles multiple dimensions", () => {
          group = crossfilter.dimension(["id", "age"]).group()
          expect(group.all()).to.eq(
            "SELECT id as key0,age as key1,COUNT(*) AS val FROM table1 GROUP BY key0, key1 ORDER BY key0,key1"
          ) // TODO syntax squished
        })
        describe("when binParams are present", () => {
          let connector
          beforeEach(function() {
            getFieldsReturnValue = [
              {
                name: "contrib_date",
                type: "DATE",
                is_array: false,
                is_dict: false
              }
            ]
            connector = {
              platform: () => "mapd",
              getFields,
              query: chai.spy((a, b, cb) => Promise.resolve(cb(null, [])))
            }
            return cf.crossfilter(connector, "contributions").then(crsfltr => {
              crossfilter = crsfltr
              dimension = crossfilter.dimension("contrib_date")
            })
          })
          it("should apply the proper binParams to the query", function() {
            return dimension
              .group()
              .binParams([
                {
                  binBounds: [new Date("1/1/2006"), new Date("1/1/2007")],
                  numBins: 400,
                  timeBin: "month"
                }
              ])
              .all(() => {
                if (isPST) {
                  expect(connector.query).to.have.been.called.with(
                    "SELECT date_trunc(month, CAST(contrib_date AS TIMESTAMP(0))) as key0,COUNT(*) AS val FROM contributions WHERE (CAST(contrib_date AS TIMESTAMP(0)) >= TIMESTAMP(0) '2006-01-01 08:00:00' AND CAST(contrib_date AS TIMESTAMP(0)) <= TIMESTAMP(0) '2007-01-01 08:00:00') GROUP BY key0 ORDER BY key0"
                  )
                } else {
                  expect(connector.query).to.have.been.called.with(
                    "SELECT date_trunc(month, CAST(contrib_date AS TIMESTAMP(0))) as key0,COUNT(*) AS val FROM contributions WHERE (CAST(contrib_date AS TIMESTAMP(0)) >= TIMESTAMP(0) '2006-01-01 00:00:00' AND CAST(contrib_date AS TIMESTAMP(0)) <= TIMESTAMP(0) '2007-01-01 00:00:00') GROUP BY key0 ORDER BY key0"
                  )
                }
              })
          })
          it("should apply the proper binParams to the query when using extract", function() {
            return dimension
              .group()
              .binParams([
                {
                  binBounds: [new Date("1/1/2006"), new Date("1/1/2007")],
                  numBins: 400,
                  timeBin: "month",
                  extract: true
                }
              ])
              .all(() => {
                expect(connector.query).to.have.been.called.with(
                  "SELECT extract(month from contrib_date) as key0,COUNT(*) AS val FROM contributions GROUP BY key0 ORDER BY key0"
                )
              })
          })
        })
      })

      // group.dimension
      describe(".dimension", () => {
        it("should return the dimension ", () => {
          expect(group.dimension()).to.equal(dimension)
        })
      })

      describe(".writeFilter", () => {
        it("returns filter statement", () => {
          dimension.filter(6)
          dimension.setDrillDownFilter(true)
          expect(group.writeFilter()).to.eq("bargle = 6")
        })
        it("returns global filter statement", () => {
          const dimension = crossfilter.dimension("bargle", true)
          dimension.filter(6)
          dimension.setDrillDownFilter(true)
          expect(dimension.group().writeFilter()).to.eq("bargle = 6")
        })
        it("AND concats filters", () => {
          dimension.filter(1)
          crossfilter.filter().filter("id = 2")
          group = crossfilter.dimension("id").group()
          expect(group.writeFilter()).to.eq("bargle = 1 AND id = 2")
        })
        it("uses args to form filter if provided", () => {
          const queryBinParams = [{ binBounds: [1, 2] }]
          expect(group.writeFilter(queryBinParams)).to.eq(
            "(bargle >= 1 AND bargle <= 2)"
          )
        })
        it("AND concats from multiple dimensions to form filter", () => {
          dimension.filter(1)
          dimension = crossfilter.dimension("id")
          group = dimension.group()
          const queryBinParams = [{ binBounds: [1, 2] }]
          expect(group.writeFilter(queryBinParams)).to.eq(
            "bargle = 1 AND (id >= 1 AND id <= 2)"
          )
        })
        it("uses rangeFilters if present", () => {
          dimension.filterRange([3, 4], null, true)
          group.setBoundByFilter(true)
          const queryBinParams = [{ binBounds: [1, 2] }]
          expect(group.writeFilter(queryBinParams)).to.eq(
            "(bargle >= 3 AND bargle <= 4)"
          )
        })
        it("returns only selfFilter if no other filters", () => {
          dimension.selfFilter("custom = 2")
          expect(group.writeFilter()).to.eq("custom = 2")
        })
        it("concats selfFilter and other filters", () => {
          dimension.selfFilter("custom = 2")
          const queryBinParams = [{ binBounds: [1, 2] }]
          expect(group.writeFilter(queryBinParams)).to.eq(
            "(bargle >= 1 AND bargle <= 2) AND custom = 2"
          )
        })
        it("prevents nulls for non-COUNT agg modes", () => {
          group.reduceAvg("age")
          expect(group.top(1)).to.eq(
            "SELECT bargle as key0,AVG(age) AS val FROM table1 WHERE age IS NOT NULL GROUP BY key0 ORDER BY val DESC LIMIT 1"
          )
        })
        it("does null COUNT measures", () => {
          group.reduceCount("lbs")
          expect(group.top(1)).to.eq(
            "SELECT bargle as key0,COUNT(lbs) AS val FROM table1 WHERE lbs IS NOT NULL GROUP BY key0 ORDER BY val DESC LIMIT 1"
          )
        })
        xit("prevents no nulls when no agg mode", () => {
          group.reduce("")
          expect(group.top(1)).to.eq(
            "SELECT bargle as key0 FROM table1 GROUP BY key0 ORDER BY val DESC LIMIT 1"
          )
        })
      })
      describe(".minMaxWithFilters", () => {
        beforeEach(function() {
          const dataConnector = {
            getFields,
            query: (q, _, callback) => callback(null, [10])
          }
          return cf.crossfilter(dataConnector, "users").then(crsfltr => {
            crossfilter = crsfltr
            dimension = crossfilter.dimension("id")
            group = dimension.group()
            dimension.projectOnAllDimensions(true)
          })
        })
        it("returns a promise", () => {
          expect(group.getMinMaxWithFilters()).to.be.an.instanceof(Promise)
        })
        it("returns the min/max", () => {
          return group.getMinMaxWithFilters().then(bounds => {
            expect(bounds).to.eq(10)
          })
        })
      })
    })
    describe(".groupAll", () => {
      it("is an alias for crossfilter.groupAll", () => {
        expect(dimension.groupAll).to.eq(crossfilter.groupAll)
      })
    })
    describe(".setEliminateNull", () => {
      it("returns own dimension object", () => {
        expect(dimension.setEliminateNull([])).to.eql(dimension)
      })
      it("sets eliminateNull", () => {
        expect(dimension.getEliminateNull()).to.eq(true)
        dimension.setEliminateNull(false)
        expect(dimension.getEliminateNull()).to.eq(false)
        dimension.setEliminateNull(true)
        expect(dimension.getEliminateNull()).to.eq(true)
      })
    })
    describe(".toggleTarget", () => {
      it("nulls targetFilter if it is the dimensionIndex", () => {
        dimension.toggleTarget()
        expect(dimension.isTargeting()).to.eq(true)
        dimension.toggleTarget()
        expect(dimension.isTargeting()).to.eq(false)
      })
      it("sets targetFilter to dimensionIndex if it's null", () => {
        expect(dimension.isTargeting()).to.eq(false)
        dimension.toggleTarget()
        expect(dimension.isTargeting()).to.eq(true)
      })
    })
    describe(".removeTarget", () => {
      it("nulls targetFilter if it is the dimensionIndex", () => {
        dimension.toggleTarget()
        expect(dimension.isTargeting()).to.eq(true)
        dimension.removeTarget()
        expect(dimension.isTargeting()).to.eq(false)
      })
    })
    describe(".allowTargeted", () => {
      it("returns own dimension object if arguments", () => {
        expect(dimension.allowTargeted(false)).to.eql(dimension)
      })
      it("returns allowTargeted if no arguments", () => {
        expect(dimension.allowTargeted()).to.eq(true)
      })
      it("can set allowTargeted", () => {
        expect(dimension.allowTargeted()).to.eq(true)
        dimension.allowTargeted(false)
        expect(dimension.allowTargeted()).to.eq(false)
      })
    })
    describe(".isTargeting", () => {
      it("returns true if targetFilter is dimensionIndex", () => {
        dimension.removeTarget()
        expect(dimension.isTargeting()).to.eq(false)
      })
      it("returns false if targetFilter is not dimensionIndex", () => {
        expect(dimension.isTargeting()).to.eq(false)
      })
    })
    describe(".dispose", () => {
      it("nulls filter and dimensions for given index", () => {
        expect(dimension.getFilterString()).to.eq("")
        expect(crossfilter.getDimensions()).to.eql(["bargle"])
        dimension.dispose()
        expect(dimension.getFilterString()).to.eq(null)
        expect(crossfilter.getDimensions()).to.eql([null])
      })
    })
    describe(".remove", () => {
      it("is an alias for dispose", () => {
        expect(dimension.remove).to.eq(dimension.dispose)
      })
    })
    describe(".value", () => {
      it("returns dimArray", () => {
        dimension = crossfilter.dimension("dim expression", true)
        expect(dimension.value()).to.eql(["dim expression"])
      })
    })
    describe(".set", () => {
      it("sets new dimArray", () => {
        dimension = crossfilter.dimension("dim expression", true)
        dimension.set(s => ["new"])
        expect(dimension.value()).to.eql(["new"])
      })
    })
    describe(".setDrillDownFilter", () => {
      it("returns own dimension object", () => {
        expect(dimension.setDrillDownFilter(1)).to.eql(dimension)
      })
      it("sets drillDownFilter", () => {
        expect(dimension.filterMulti([1, 2], null, null)).to.eql(dimension)
        expect(dimension.getFilterString()).to.eq("(bargle = 1 OR bargle = 2)")
        dimension.setDrillDownFilter(true)
        expect(dimension.filterMulti([1, 2], null, null)).to.eql(dimension)
        expect(dimension.getFilterString()).to.eq("(bargle = 1 AND bargle = 2)")
        dimension.setDrillDownFilter(false)
        expect(dimension.filterMulti([1, 2], null, null)).to.eql(dimension)
        expect(dimension.getFilterString()).to.eq("(bargle = 1 OR bargle = 2)")
      })
    })
  })
  describe(".groupAll", () => {
    let group
    beforeEach(() => {
      group = crossfilter.groupAll()
    })
    xit("returns group object", () => {
      expect(crossfilter.groupAll()).to.eql(group)
    })
    describe(".getCrossfilter", () => {
      it("returns parent crossfilter", () => {
        expect(group.getCrossfilter()).to.eq(crossfilter)
      })
    })
    describe(".getCrossfilterId", () => {
      it("returns parent crossfilter's id", () => {
        expect(group.getCrossfilterId()).to.eq(crossfilter.getId())
      })
    })
    describe(".getTable", () => {
      it("returns table", () => {
        expect(group.getTable()).to.eql(crossfilter.getTable())
      })
    })
    describe(".reduceCount", () => {
      it("returns own group object", () => {
        expect(group.reduceCount()).to.eql(group)
      })
      it("sets reduceExpression to count(*) with no arg", () => {
        group.reduceCount()
        expect(group.getReduceExpression()).to.eq("COUNT(*) as val")
      })
      it("sets reduceExpression to based on arg", () => {
        group.reduceCount("x.id")
        expect(group.getReduceExpression()).to.eq("COUNT(x.id) as val")
      })
      it("sets reduceExpression to based on arg with custom name", () => {
        group.reduceCount("x.id", "idcount")
        expect(group.getReduceExpression()).to.eq("COUNT(x.id) as idcount")
      })
    })
    describe(".reduceSum", () => {
      it("returns own group object", () => {
        expect(group.reduceSum()).to.eql(group)
      })
      xit("validates input")
      it("sets reduceExpression", () => {
        group.reduceSum("x.id")
        expect(group.getReduceExpression()).to.eq("SUM(x.id) as val")
      })
      it("sets reduceExpression with custom name", () => {
        group.reduceSum("x.id", "idsum")
        expect(group.getReduceExpression()).to.eq("SUM(x.id) as idsum")
      })
    })
    describe(".reduceAvg", () => {
      it("returns own group object", () => {
        expect(group.reduceAvg()).to.eql(group)
      })
      xit("validates input")
      it("sets reduceExpression", () => {
        group.reduceAvg("x.id")
        expect(group.getReduceExpression()).to.eq("AVG(x.id) as val")
      })
      it("sets reduceExpression with custom name", () => {
        group.reduceAvg("x.id", "idavg")
        expect(group.getReduceExpression()).to.eq("AVG(x.id) as idavg")
      })
    })
    describe(".reduceMin", () => {
      it("returns own group object", () => {
        expect(group.reduceMin()).to.eql(group)
      })
      xit("validates input")
      it("sets reduceExpression", () => {
        group.reduceMin("x.id")
        expect(group.getReduceExpression()).to.eq("MIN(x.id) as val")
      })
      it("sets reduceExpression with custom name", () => {
        group.reduceMin("x.id", "idmin")
        expect(group.getReduceExpression()).to.eq("MIN(x.id) as idmin")
      })
    })
    describe(".reduceMax", () => {
      it("returns own group object", () => {
        expect(group.reduceMax()).to.eql(group)
      })
      xit("validates input")
      it("sets reduceExpression", () => {
        group.reduceMax("x.id")
        expect(group.getReduceExpression()).to.eq("MAX(x.id) as val")
      })
      it("sets reduceExpression with custom name", () => {
        group.reduceMax("x.id", "idmax")
        expect(group.getReduceExpression()).to.eq("MAX(x.id) as idmax")
      })
    })
    describe(".reduce", () => {
      it("returns own group object", () => {
        expect(group.reduce([])).to.eql(group)
      })
      xit("validates input")
      it("sets reduceExpression", () => {
        const expressions = [
          { expression: "x.id", agg_mode: "MIN", name: "minx" }
        ]
        group.reduce(expressions)
        expect(group.getReduceExpression()).to.eq("MIN(x.id) AS minx")
      })
      it("sets reduceExpression when agg_mode is COUNT", () => {
        const expressions = [
          { expression: "x.id", agg_mode: "COUNT", name: "minx" }
        ]
        group.reduce(expressions)
        expect(group.getReduceExpression()).to.eq("COUNT(x.id) AS minx")
      })
      it("sets reduceExpression when agg_mode is COUNT and no expression", () => {
        const expressions = [{ agg_mode: "COUNT", name: "minx" }]
        group.reduce(expressions)
        expect(group.getReduceExpression()).to.eq("COUNT(*) AS minx")
      })
      it("sets reduceExpression for multiple expressions", () => {
        const expressions = [
          { expression: "x.id", agg_mode: "AVG", name: "avgx" },
          { expression: "y.rx", agg_mode: "MAX", name: "maxy" }
        ]
        group.reduce(expressions)
        expect(group.getReduceExpression()).to.eq(
          "AVG(x.id) AS avgx,MAX(y.rx) AS maxy"
        )
      })
    })
    describe(".reduceMulti", () => {
      it("is an alias for .reduce", () => {
        expect(group.reduce).to.eq(group.reduceMulti)
      })
    })
    describe(".value", () => {
      it("returns value of query result", function() {
        const dataConnector = { getFields, query: _ => [{ val: 2, other: 1 }] }
        return crossfilter
          .setDataAsync(dataConnector, "table1")
          .then(crsfltr => {
            expect(crsfltr.groupAll().value()).to.eq(2)
          })
      })
      it("returns query result ignoring filters", function() {
        const dataConnector = { getFields, query: _ => [{ val: 2, other: 1 }] }
        return crossfilter
          .setDataAsync(dataConnector, "table1", [])
          .then(crsfltr => {
            expect(crsfltr.groupAll().value(true, true)).to.eq(2)
          })
      })
      it("returns query result not ignoring global filters", function() {
        const dataConnector = { getFields, query: _ => [{ val: 2, other: 1 }] }
        return crossfilter
          .setDataAsync(dataConnector, "table1", [])
          .then(crsfltr => {
            crossfilter = crsfltr
            crossfilter.filter(true).filter("age < 10")
            crossfilter.filter().filter("age > 10")
            expect(crsfltr.groupAll().value(false, true)).to.eq(2)
          })
      })
      it("returns query result not ignoring chart filters", function() {
        const dataConnector = { getFields, query: _ => [{ val: 2, other: 1 }] }
        return crossfilter
          .setDataAsync(dataConnector, "table1", [])
          .then(crsfltr => {
            crossfilter = crsfltr
            crossfilter.filter().filter("age < 10")
            crossfilter.filter(true).filter("age > 10")
            expect(crsfltr.groupAll().value(true, false)).to.eq(2)
          })
      })
      it("returns query result not ignoring chart and global filters", function() {
        const dataConnector = { getFields, query: _ => [{ val: 2, other: 1 }] }
        return crossfilter
          .setDataAsync(dataConnector, "table1", [])
          .then(crsfltr => {
            crossfilter = crsfltr
            crossfilter.filter().filter("age < 10")
            crossfilter.filter(true).filter("age > 10")
            expect(crsfltr.groupAll().value(false, false)).to.eq(2)
          })
      })
      it("returns query result not ignoring empty chart and global filters", function() {
        const dataConnector = { getFields, query: _ => [{ val: 2, other: 1 }] }
        return crossfilter
          .setDataAsync(dataConnector, "table1", [])
          .then(crsfltr => {
            crossfilter = crsfltr
            crossfilter.filter().filter("")
            crossfilter.filter(true).filter("")
            expect(crsfltr.groupAll().value(false, false)).to.eq(2)
          })
      })
      it("returns query result with joins", function() {
        const dataConnector = { getFields, query: _ => [{ val: 2, other: 1 }] }
        return crossfilter
          .setDataAsync(dataConnector, "table1", [])
          .then(crsfltr => {
            expect(crsfltr.groupAll().value()).to.eq(2)
          })
      })
      it("returns query result with joins and filter", function() {
        const dataConnector = { getFields, query: _ => [{ val: 2, other: 1 }] }
        return crossfilter
          .setDataAsync(dataConnector, "table1", [])
          .then(crsfltr => {
            crossfilter = crsfltr
            crossfilter.filter().filter("age < 10")
            expect(crossfilter.groupAll().value()).to.eq(2)
          })
      })
      it("returns query result with joins and multiple filters", function() {
        const dataConnector = { getFields, query: _ => [{ val: 2, other: 1 }] }
        return crossfilter
          .setDataAsync(dataConnector, "table1", [])
          .then(crsfltr => {
            crossfilter = crsfltr
            crossfilter.filter().filter("age < 10")
            crossfilter.filter().filter("id = 456")
            expect(crossfilter.groupAll().value()).to.eq(2)
          })
      })
    })
    describe(".valueAsync", () => {
      it("returns promise that resolves to value of query result", () => {
        const dataConnector = {
          getFields,
          query: (q, con, cb) => cb(null, [{ val: 2, other: 1 }])
        }
        crossfilter.setDataAsync(dataConnector, "table1").then(crsfltr => {
          crsfltr
            .groupAll()
            .valueAsync()
            .then(val => {
              expect(val).to.eq(2)
            })
        })
      })
    })
    describe(".values", () => {
      it("returns full query result", function() {
        const dataConnector = { getFields, query: _ => [{ val: 2, other: 1 }] }
        return crossfilter
          .setDataAsync(dataConnector, "table1")
          .then(crsfltr => {
            expect(crsfltr.groupAll().values()).to.eql({ val: 2, other: 1 })
          })
      })
    })
    describe(".valuesAsync", () => {
      it("returns promise that resolves to values of query result", () => {
        const dataConnector = {
          getFields,
          query: (q, con, cb) => cb(null, [{ val: 2, other: 1 }])
        }
        crossfilter.setDataAsync(dataConnector, "table1").then(crsfltr => {
          crsfltr
            .groupAll()
            .valuesAsync()
            .then(val => {
              expect(val).to.eql({ val: 2, other: 1 })
            })
        })
      })
    })
  })
  describe(".size", () => {
    it("returns number of records", () => {
      crossfilter.setDataAsync(
        { getFields: () => [], query: () => [{ n: 123 }] },
        null,
        []
      )
      expect(crossfilter.size()).to.eql(123)
    })
    xit("constructs a valid query when _joinStmt is undefined")
  })
  describe(".getFilter", () => {
    it("returns unset filters", () => {
      expect(crossfilter.getFilter()).to.eql([])
    })
    xit("returns set filters", () => {
      crossfilter.setDataAsync({ getFields: () => [] }, null, [])
      expect(crossfilter.getFilter()).to.eql(["a filter"])
    })
  })
  describe(".getFilterString", () => {
    it("returns filter", () => {
      crossfilter.filter().filter("age > 35")
      expect(crossfilter.getFilterString()).to.eq("age > 35")
    })
    it("combines multiple filters", () => {
      crossfilter.filter().filter("x")
      crossfilter.filter().filter("y")
      expect(crossfilter.getFilterString()).to.eq("x AND y")
    })
    it("handles empty filters", () => {
      crossfilter.filter().filter("")
      expect(crossfilter.getFilterString()).to.eq("")
    })
  })
  describe(".getGlobalFilterString", () => {
    it("returns filter", () => {
      crossfilter.setGlobalFilter(["age > 35"])
      expect(crossfilter.getGlobalFilterString()).to.eq("age > 35")
      crossfilter.setGlobalFilter(() => ["age > 40"])
      expect(crossfilter.getGlobalFilterString()).to.eq("age > 40")
    })
    it("combines multiple filters", () => {
      crossfilter.filter(true).filter("x")
      crossfilter.filter(true).filter("y")
      expect(crossfilter.getGlobalFilterString()).to.eq("x AND y")
    })
    it("handles empty filters", () => {
      crossfilter.filter(true).filter("")
      expect(crossfilter.getGlobalFilterString()).to.eq("")
    })
  })
  describe(".getDimensions", () => {
    it("returns unset dimensions", () => {
      expect(crossfilter.getDimensions()).to.eql([])
    })
    it("returns set dimensions", () => {
      crossfilter.setDataAsync({ getFields: () => [] }, null, [])
      crossfilter.dimension("a dimension")
      expect(crossfilter.getDimensions()).to.eql(["a dimension"])
    })
  })
  describe(".getTable", () => {
    it("returns unset data tables", () => {
      expect(crossfilter.getTable()).to.eq(null)
    })
    it("returns set data tables", () => {
      crossfilter.setDataAsync({ getFields: () => [] }, "a table", [])
      expect(crossfilter.getTable()).to.eql(["a table"])
    })
  })
})
describe("resultCache", () => {
  let resultCache
  beforeEach(() => {
    resultCache = cf.resultCache()
  })
  describe(".query", () => {
    it("returns the data synchronously", () => {
      resultCache.setDataConnector({ query: () => 123 })
      expect(resultCache.query("select *;")).to.eq(123)
    })
    it("hits cache if possible", () => {
      resultCache.setDataConnector({ query: () => 123 })
      expect(resultCache.query("select *;")).to.eq(123)
      resultCache.setDataConnector({ query: () => 45 })
      expect(resultCache.query("select *;")).to.eq(123)
      expect(resultCache.query("select (*);")).to.eq(45)
    })
    it("evicts oldest cache entry if necessary", () => {
      resultCache.setDataConnector({ query: n => n })
      resultCache.setMaxCacheSize(2)
      resultCache.query("1")
      resultCache.query("2")
      expect(resultCache.peekAtCache().cache).to.eql({
        1: { time: 0, data: "1", showNulls: false },
        2: { time: 1, data: "2", showNulls: false }
      })
      resultCache.query("3")
      expect(resultCache.peekAtCache().cache).to.eql({
        2: { time: 1, data: "2", showNulls: false },
        3: { time: 2, data: "3", showNulls: false }
      })
    })
    it("post-processes data if necessary", () => {
      resultCache.setDataConnector({ query: () => 2 })
      const options = {
        postProcessors: [x => x + 1, x => x * 2]
      }
      expect(resultCache.query("select *;", options)).to.eq(6)
    })
    it("does not check cache if renderSpec true", () => {
      const options = { renderSpec: true }
      resultCache.setDataConnector({ query: n => n })
      resultCache.query("1")
      expect(resultCache.peekAtCache().cache).to.eql({
        1: { time: 0, data: "1", showNulls: false }
      })
      resultCache.setDataConnector({ query: n => 10 * n })
      expect(resultCache.query("1", options)).to.eql(10)
    })
    it("does not store in cache if renderSpec true", () => {
      const options = { renderSpec: true }
      resultCache.setDataConnector({ query: n => n })
      resultCache.query("1")
      expect(resultCache.peekAtCache().cache).to.eql({
        1: { time: 0, data: "1", showNulls: false }
      })
      resultCache.setDataConnector({ query: n => 10 * n })
      resultCache.query("1", options)
      expect(resultCache.peekAtCache().cache).to.eql({
        1: { time: 0, data: "1", showNulls: false }
      })
    })
    it("does not evict from cache if renderSpec true", () => {
      const options = { renderSpec: true }
      resultCache.setDataConnector({ query: n => n })
      resultCache.setMaxCacheSize(2)
      resultCache.query("1")
      resultCache.query("2")
      resultCache.query("3", options)
      expect(resultCache.peekAtCache().cache).to.eql({
        1: { time: 0, data: "1", showNulls: false },
        2: { time: 1, data: "2", showNulls: false }
      })
    })
  })
  describe(".queryAsync", () => {
    it("returns the data asynchronously", done => {
      resultCache.setDataConnector({ query: (qry, opt, cbs) => cbs() })
      resultCache.queryAsync("select *", {}, done)
    })
    it("hits cache if possible", () => {
      // if renderSpec is falsey
      resultCache.setDataConnector({ query: (qry, opt, cb) => cb(null, 123) })
      resultCache.queryAsync("a", {}, () => {})
      expect(resultCache.peekAtCache().cache).to.eql({
        a: { time: 0, data: 123, showNulls: false }
      })
      resultCache.queryAsync("a", {}, () => {})
      expect(resultCache.peekAtCache().cache).to.eql({
        a: { time: 2, data: 123, showNulls: false }
      }) // TODO why is time skipping 1?
    })
    it("evicts oldest cache entry if necessary", () => {
      resultCache.setMaxCacheSize(2)
      resultCache.setDataConnector({ query: (qry, opt, cb) => cb(null, qry) })
      resultCache.queryAsync("a", {}, () => {})
      resultCache.queryAsync("b", {}, () => {})
      expect(resultCache.peekAtCache().cache).to.eql({
        a: { time: 0, data: "a", showNulls: false },
        b: { time: 1, data: "b", showNulls: false }
      })
      resultCache.queryAsync("a", {}, () => {})
      resultCache.queryAsync("c", {}, () => {})
      expect(resultCache.peekAtCache().cache).to.eql({
        a: { time: 3, data: "a", showNulls: false },
        c: { time: 4, data: "c", showNulls: false }
      }) // TODO why is time skipping 2?
    })
    it("post-processes data if necessary", () => {
      const callback = x => x //{
      // TODO callback not being called with value after postProcessors
      // if(x===5){ }
      // }
      resultCache.setDataConnector({ query: (qry, opt, cb) => cb(null, 1) })
      const options = {
        postProcessors: [x => x * 2, x => x + 3]
      }
      resultCache.queryAsync("a", options, callback)
      expect(resultCache.peekAtCache().cache).to.eql({
        a: { time: 0, data: 5, showNulls: false }
      })
    })
    xit("does not check cache if renderSpec true", () => {
      const options = { renderSpec: true }
      resultCache.setDataConnector({ query: n => n })
      resultCache.queryAsync("1")
      expect(resultCache.peekAtCache().cache).to.eql({
        1: { time: 0, data: "1" }
      })
      resultCache.setDataConnector({ query: n => 10 * n })
      expect(resultCache.queryAsync("1", options)).to.eql(10)
    })
    xit("does not store in cache if renderSpec true", () => {
      const options = { renderSpec: true }
      resultCache.setDataConnector({ query: n => n })
      resultCache.queryAsync("1")
      expect(resultCache.peekAtCache().cache).to.eql({
        1: { time: 0, data: "1" }
      })
      resultCache.setDataConnector({ query: n => 10 * n })
      resultCache.queryAsync("1", options)
      expect(resultCache.peekAtCache().cache).to.eql({
        1: { time: 0, data: "1" }
      })
    })
    xit("does not evict from cache if renderSpec true", () => {
      const options = { renderSpec: true }
      resultCache.setDataConnector({ query: n => n })
      resultCache.setMaxCacheSize(2)
      resultCache.queryAsync("1")
      resultCache.queryAsync("2")
      resultCache.queryAsync("3", options)
      expect(resultCache.peekAtCache().cache).to.eql({
        1: { time: 0, data: "1" },
        2: { time: 1, data: "2" }
      })
    })
  })
  describe(".emptyCache", () => {
    it("returns itself with an empty cache", () => {
      resultCache.setDataConnector({ query: () => 1 })
      resultCache.query("a")
      expect(resultCache.peekAtCache().cache).to.eql({
        a: { time: 0, data: 1, showNulls: false }
      })
      resultCache.emptyCache()
      expect(resultCache.peekAtCache().cache).to.eql({})
    })
  })
  describe(".setMaxCacheSize", () => {
    it("sets maxCacheSize", () => {
      expect(resultCache.getMaxCacheSize()).to.eq(10)
      resultCache.setMaxCacheSize(123)
      expect(resultCache.getMaxCacheSize()).to.eq(123)
    })
  })
  describe(".setDataConnector", () => {
    it("sets the data connector", () => {
      expect(resultCache.getDataConnector()).to.eq(undefined)
      resultCache.setDataConnector({ the: 1 })
      expect(resultCache.getDataConnector()).to.eql({ the: 1 })
    })
  })
})
describe("filterNullMeasures", () => {
  it("filters out a null measure", () => {
    const columns = [{ expression: "id", agg_mode: "SUM" }]
    expect(cf.filterNullMeasures("", columns)).to.eq("id IS NOT NULL")
  })
  it("filters out multiple null measures", () => {
    const columns = [
      { expression: "id", agg_mode: "min" },
      { expression: "age", agg_mode: "avg" }
    ]
    expect(cf.filterNullMeasures("", columns)).to.eq(
      "id IS NOT NULL AND age IS NOT NULL"
    )
  })
  it("filters out null measures with existing filter statement", () => {
    const columns = [
      { expression: "id", agg_mode: "count" },
      { expression: "age", agg_mode: "avg" }
    ]
    expect(cf.filterNullMeasures("1 < 2", columns)).to.eq(
      "1 < 2 AND id IS NOT NULL AND age IS NOT NULL"
    )
  })
})
describe("not empty (for filtering)", () => {
  it("considers no arguments empty", () => {
    expect(cf.notEmpty()).to.eq(false)
  })
  it("considers undefined empty", () => {
    expect(cf.notEmpty(undefined)).to.eq(false)
  })
  it("considers null empty", () => {
    expect(cf.notEmpty(null)).to.eq(false)
  })
  it("considers empty string empty", () => {
    expect(cf.notEmpty("")).to.eq(false)
  })
  it("considers empty array empty", () => {
    expect(cf.notEmpty([])).to.eq(false)
  })
  it("considers empty object empty", () => {
    expect(cf.notEmpty({})).to.eq(false)
  })
  it("considers string not empty", () => {
    expect(cf.notEmpty("false")).to.eq(true)
  })
  it("considers number not empty", () => {
    expect(cf.notEmpty(0)).to.eq(true)
    expect(cf.notEmpty(NaN)).to.eq(true)
  })
  it("considers boolean not empty", () => {
    expect(cf.notEmpty(false)).to.eq(true)
  })
  it("considers object with prop not empty", () => {
    expect(cf.notEmpty({ a: 1 })).to.eq(true)
    expect(cf.notEmpty({ length: 0 })).to.eq(true)
    expect(cf.notEmpty({ length: false })).to.eq(true)
  })
  it("considers date not empty", () => {
    expect(cf.notEmpty(new Date())).to.eq(true)
  })
  it("considers symbol not empty", () => {
    expect(cf.notEmpty(Symbol())).to.eq(true)
  })
  it("considers function not empty", () => {
    expect(cf.notEmpty(function x() {})).to.eq(true)
  })
})

describe("Parse parenthesis() for custom expressions", () => {
  it("will input a string and return an array", () => {
    expect(cf.parseParensIfExist("")).to.deep.eq([""])
  })

  it("will not parse if there are no parans", () => {
    expect(cf.parseParensIfExist("flights")).to.deep.eq(["flights"])
  })

  it("will parse if there are parans", () => {
    expect(cf.parseParensIfExist("avg(flights)")).to.deep.eq(["flights"])
  })

  it("will only parse the outer layer of parans", () => {
    expect(cf.parseParensIfExist("avg(flights - avg(arrdelay))")).to.deep.eq([
      "flights - avg(arrdelay)"
    ])
  })
})

describe("replaceRelative", () => {
  it("replaces NOW() with TIMESTAMP", () => {
    expect(replaceRelative("NOW()")).to.include("TIMESTAMP(0) '")
  })

  it("replaces 'DATE_ADD(days, 1, NOW())' with TIMESTAMP", () => {
    expect(replaceRelative("DATE_ADD(days, 1, NOW())")).to.include(
      "TIMESTAMP(0) '"
    )
  })

  it("replaces 'DATE_ADD(days, DATEDIFF(days, NOW,  0), NOW())' with TIMESTAMP", () => {
    expect(
      replaceRelative("DATE_ADD(days, DATEDIFF(days, 0, NOW()), NOW())")
    ).to.include("TIMESTAMP(0) '")
  })

  it("replaces 'DATE_ADD(days, DATEDIFF(days, NOW,  0)-2, NOW())' with TIMESTAMP", () => {
    expect(
      replaceRelative("DATE_ADD(days, DATEDIFF(days, 0, NOW())-2, NOW())")
    ).to.include("TIMESTAMP(0) '")
  })
})
