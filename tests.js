"use strict"
const expect = require("chai").expect
const cf = require("./mapd-crossfilter")

// TODO either remove or fix the append options to filters
describe("crossfilter", () => {
  let crossfilter
  beforeEach(() => {
    crossfilter = cf.crossfilter()
  })
  it("has a version", () => {
    expect(cf.crossfilter.version).to.eq("1.3.11")
  })
  it("has a type", () => {
    expect(crossfilter.type).to.eq("crossfilter")
  })
  it("can be invoked to setData and return self", () => {
    const dataConnector = {getFields: _ => []}
    const dataTables = "a table"
    const crsfltr = cf.crossfilter(dataConnector, dataTables)
    expect(crsfltr.type).to.eql("crossfilter")
    expect(crsfltr.getTable()).to.eql(["a table"])
  })
  describe(".setData", () => {
    it("selects from multiple tables", () => {
      const dataConnector = {getFields: _ => [], query:_ => _}
      const dataTables = ["tableA", "tableB"]
      crossfilter.setData(dataConnector, dataTables)
      crossfilter.size()
      expect(crossfilter.peekAtCache()).to.have.key("SELECT COUNT(*) as n FROM tableA,tableB")
    })
    it("joins tables", () => {
      const dataConnector = {getFields: _ => [], query:_ => _}
      const dataTables = "table1"
      const joinAttrs = [{table1:"table1", table2:"table2", attr1:"id", attr2:"x_id"}]
      crossfilter.setData(dataConnector, dataTables, joinAttrs)
      crossfilter.size()
      expect(crossfilter.peekAtCache()).to.have.key("SELECT COUNT(*) as n FROM table1 WHERE table1.id = table2.x_id")
    })
    it("joins multiple tables", () => {
      const dataConnector = {getFields: _ => [], query:_ => _}
      const dataTables = "table1"
      const joinAttrs = [
        {table1:"table1", table2:"table2", attr1:"id", attr2:"x_id"},
        {table1:"table2", table2:"table3", attr1:"id", attr2:"y_id"}
      ]
      crossfilter.setData(dataConnector, dataTables, joinAttrs)
      crossfilter.size()
      expect(crossfilter.peekAtCache()).to.have.key("SELECT COUNT(*) as n FROM table1 WHERE table1.id = table2.x_id AND table2.id = table3.y_id")
    })
    xit("joins the same way regardless of order", () => {
      const dataConnector = {getFields: _ => [], query:_ => _}
      const dataTables = "table1"
      const joinAttrs = [
        {table1:"table2", table2:"table1", attr1:"x_id", attr2:"id"},
        {table1:"table1", table2:"table2", attr1:"id", attr2:"x_id"}
      ]
      crossfilter.setData(dataConnector, dataTables, joinAttrs)
      crossfilter.size()
      expect(crossfilter.peekAtCache()).to.have.key("SELECT COUNT(*) as n FROM table1 WHERE table1.id = table2.x_id AND table2.id = table3.y_id")
    })
    it("identifies ambiguous table columns", () => {
      const columnsArray = [
        {name:"age", type:"idk", is_array:false, is_dict:false},
        {name:"age", type:"other", is_array:false, is_dict:false}
      ]
      const dataConnector = {getFields:_ => columnsArray}
      crossfilter.setData(dataConnector, "tableA")
      expect(crossfilter.getColumns()).to.eql({
        "tableA.age": {
          table: "tableA",
          column: "age",
          type: "other",
          is_array: false,
          is_dict: false,
          name_is_ambiguous: true
        }
      })
    })
  })
  describe(".filter", () => {
    let filter
    beforeEach(() => {
      filter = crossfilter.filter()
    })
    describe(".filter", () => {
      it("adds a filter", () => {
        filter.filter("x")
        expect(crossfilter.getFilter()).to.eql(["x"])
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
    })
    describe(".getFilter", () => {
      it("returns current filter", () => {
        const filter = crossfilter.filter()
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
    it("keeps track of table columns", () => {
      const columnsArray = [
        {name:"age", type:"idk", is_array:false, is_dict:false},
        {name:"sex", type:"idk", is_array:false, is_dict:false}
      ]
      const dataConnector = {getFields:_ => columnsArray}
      crossfilter.setData(dataConnector, "tableA")
      expect(crossfilter.getColumns()).to.eql({
        "tableA.age": {
          table: "tableA",
          column: "age",
          type: "idk",
          is_array: false,
          is_dict: false,
          name_is_ambiguous: false
        },
        "tableA.sex": {
          table: "tableA",
          column: "sex",
          type: "idk",
          is_array: false,
          is_dict: false,
          name_is_ambiguous: false
        },
      })
    })
  })
  describe(".dimension", () => {
    let dimension
    beforeEach(() => {
      crossfilter = crossfilter.setData({}, [])
      dimension = crossfilter.dimension("bargle")
    })
    it("returns itself", () => {
      expect(crossfilter.dimension().type).to.eq("dimension")
    })
    describe(".order", () => {
      it("returns own dimension object", () => {
        expect(dimension.order()).to.eq(dimension)
      })
      it("sets orderExpression", () => {
        const dataConnector = {getFields: _ => [], query: _ => _}
        dimension = crossfilter.setData(dataConnector, "table1").dimension("bargle")
        dimension.order("created_at")
        dimension.projectOnAllDimensions(true)
        dimension.top(1, 1)
        expect(dimension.getResultSet()).to.eq("SELECT bargle FROM table1 ORDER BY created_at DESC LIMIT 1 OFFSET 1")
      })
    })
    describe(".orderNatural", () => {
      it("returns own dimension object", () => {
        expect(dimension.orderNatural()).to.eq(dimension)
      })
      it("nulls out orderExpression", () => {
        const dataConnector = {getFields: _ => [], query: _ => _}
        dimension = crossfilter.setData(dataConnector, "table1").dimension("bargle")
        dimension.projectOnAllDimensions(true)
        dimension.order("created_at")
        dimension.top(1, 1)
        expect(dimension.getResultSet()).to.eq("SELECT bargle FROM table1 ORDER BY created_at DESC LIMIT 1 OFFSET 1")
        dimension.orderNatural()
        dimension.top(1, 1)
        expect(dimension.getResultSet()).to.eq("SELECT bargle FROM table1 ORDER BY bargle DESC LIMIT 1 OFFSET 1")
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
      it("filters query", () => {
        const dataConnector = {getFields: _ => [], query: _ => _}
        dimension = cf.crossfilter(dataConnector, "table1").dimension("age")
        dimension.selfFilter("admin = true")
        dimension.projectOnAllDimensions(true)
        dimension.top(1, 1)
        expect(dimension.getResultSet()).to.eq("SELECT age FROM table1 WHERE admin = true ORDER BY age DESC LIMIT 1 OFFSET 1")
      })
      it("appends to existing filters", () => {
        const dataConnector = {getFields: _ => [], query: _ => _}
        dimension = cf.crossfilter(dataConnector, "table1").dimension("age")
        dimension.selfFilter("admin = true")
        dimension.filter(35)
        dimension.projectOnAllDimensions(true)
        dimension.top(1, 1)
        expect(dimension.getResultSet()).to.eq("SELECT age FROM table1 WHERE age = 35 AND admin = true ORDER BY age DESC LIMIT 1 OFFSET 1")
      })
    })
    describe(".filter", () => {
      it("escapes apostrophes in filter values", () => {
        const jqueryStub = _ => ({trigger: _ => _})
        dimension.filterExact("McDonald's")
        expect(dimension.getFilterString()).to.eq("bargle = 'McDonald''s'")
        dimension.filterAll(null, jqueryStub)
        dimension.filterExact("'til later")
        expect(dimension.getFilterString()).to.eq("bargle = '''til later'")
        dimension.filterAll(null, jqueryStub)
        dimension.filterExact("ye ol'")
        expect(dimension.getFilterString()).to.eq("bargle = 'ye ol'''")
        dimension.filterAll(null, jqueryStub)
        dimension.filterExact("Wouldn't've")
        expect(dimension.getFilterString()).to.eq("bargle = 'Wouldn''t''ve'")
      })
      it("escapes percent signs in filter values", () => {
        const jqueryStub = _ => ({trigger: _ => _})
        dimension.filterExact("100%")
        expect(dimension.getFilterString()).to.eq("bargle = '100\\%'")
        dimension.filterAll(null, jqueryStub)
        dimension.filterExact("% change")
        expect(dimension.getFilterString()).to.eq("bargle = '\\% change'")
        dimension.filterAll(null, jqueryStub)
        dimension.filterExact("%%")
        expect(dimension.getFilterString()).to.eq("bargle = '\\%\\%'")
      })
      it("escapes underscores in filter values", () => {
        const jqueryStub = _ => ({trigger: _ => _})
        dimension.filterExact("100_")
        expect(dimension.getFilterString()).to.eq("bargle = '100\\_'")
        dimension.filterAll(null, jqueryStub)
        dimension.filterExact("_ change")
        expect(dimension.getFilterString()).to.eq("bargle = '\\_ change'")
        dimension.filterAll(null, jqueryStub)
        dimension.filterExact("__")
        expect(dimension.getFilterString()).to.eq("bargle = '\\_\\_'")
      })
      it("returns filterAll if range is null", () => {
        const jqueryStub = _ => ({trigger: _ => _})
        dimension.filterExact("something")
        expect(dimension.getFilterString()).to.eq("bargle = 'something'")
        dimension.filterAll(null, jqueryStub)
        expect(dimension.getFilterString()).to.eq("")
        dimension.filterExact("something")
        expect(dimension.getFilterString()).to.eq("bargle = 'something'")
        dimension.filter(null, null, null, null, jqueryStub)
        expect(dimension.getFilterString()).to.eq("")
      })
      it("returns filterRange if range is array and multiDim is falsey", () => {
        dimension.filterRange([1,2], undefined, undefined)
        expect(dimension.getFilterString()).to.eq("(bargle >= 1 AND bargle < 2)")
        dimension.filter([1,2], null, null)
        expect(dimension.getFilterString()).to.eq("(bargle >= 1 AND bargle < 2)")
      })
      xit("returns filterFunction if (range isn't array or multiDim is truthy) and range is a function")
      it("returns filterExact (range isn't array or multiDim is truthy) and range isn't a function", () => {
        dimension.filterExact("a range", undefined)
        expect(dimension.getFilterString()).to.eq("bargle = 'a range'")
        dimension.filter("a range", null, null)
        expect(dimension.getFilterString()).to.eq("bargle = 'a range'")
      })
    })
    describe(".filterExact", () => {
      it("returns own dimension object", () => {
        expect(dimension.filterExact()).to.eql(dimension)
      })
      it("AND concats subExpressions")
      it("converts dates", () => {
        dimension = crossfilter.dimension(["age", "sex", "created_at"])
        dimension.filterExact([50,'f', new Date("2016-01-01")])
        expect(dimension.getFilterString()).to.eq("age = 50 AND sex = 'f' AND created_at = TIMESTAMP(0) '2016-01-01 00:00:00'")
      })
      it("uses ANY if dim contains array", () => {
        const columnsArray = [{name:"age", type:"idk", is_array:true, is_dict:false}]
        crossfilter.setData({getFields:_ => columnsArray}, "tableA")
        dimension = crossfilter.dimension(["tableA.age", "sex", "created_at"])
        dimension.filterExact([50,'f', new Date("2016-01-01")])
        expect(dimension.getFilterString()).to.eq("50 = ANY tableA.age AND sex = 'f' AND created_at = TIMESTAMP(0) '2016-01-01 00:00:00'")
      })
      it("does not use ANY if dim does not contain array", () => {
        dimension.filterExact([50])
        expect(dimension.getFilterString()).to.eq("bargle = 50")
      })
      it("sets the current filter if not appending", () => {
        dimension = crossfilter.dimension(["age", "sex"])
        dimension.filterExact([50,'f'])
        dimension.filterExact([100,'m'])
        expect(dimension.getFilterString()).to.eq("age = 100 AND sex = 'm'")
      })
      it("adds to current filter if appending", () => {
        dimension = crossfilter.dimension(["age", "sex"])
        dimension.filterExact([50,'f'])
        dimension.filterExact([100,'m'], true)
        expect(dimension.getFilterString()).to.eq("age = 50 AND sex = 'f'age = 100 AND sex = 'm'") // TODO invalid SQL? yes
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
        dimension.filterRange([1,2])
        dimension.filterRange([3,4], false)
        expect(dimension.getFilterString()).to.eq("(bargle >= 3 AND bargle < 4)")
      })
      it("adds to current filter if appending", () => {
        dimension.filterRange([1,2])
        dimension.filterRange([3,4], true)
        expect(dimension.getFilterString()).to.eq("(bargle >= 1 AND bargle < 2)(bargle >= 3 AND bargle < 4)") // TODO valid SQL?
      })
      it("converts range to list if not already one", () => {
        dimension.filterRange("not a list")
        expect(dimension.getFilter()).to.eql(["not a list"])
      })
      xit("resets range if resetRange truthy", () => {
        dimension.filterRange([1,2])
        expect(dimension.getFilter()).to.eql([[1,2]])
        expect(dimension.getFilterString()).to.eq("(bargle >= 1 AND bargle < 2)")
        dimension.filterRange([3,4], false, false)
        expect(dimension.getFilter()).to.eql([[3,4]])
        expect(dimension.getFilterString()).to.eq("(bargle >= 1 AND bargle < 2)")
      })
      it("combines subExpressions with AND", () => {
        dimension = crossfilter.dimension(["argle", "bargle"])
        dimension.filterRange([[1, 2], [3, 4]])
        expect(dimension.getFilterString()).to.eq("(argle >= 1 AND argle < 2 AND bargle >= 3 AND bargle < 4)")
      })
    })
    describe(".filterAll", () => {
      it("returns own dimension object", () => {
        expect(dimension.filterAll(true)).to.eql(dimension)
      })
      xit("triggers a jQuery 'filter-clear' and clears rangeFilters if softFilterClear is falsey", done => {
        const $ = {trigger: done}
        dimension.filterRange(123)
        expect(rangeFilters).to.eql([1])
        dimension.filterAll()
        expect(rangeFilters).to.eql([])
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
        const jqueryStub = _ => ({trigger: _ => _})
        expect(dimension.filterMulti([], null, null, jqueryStub)).to.eql(dimension)
      })
      it("triggers filter-on if filter changed from null to not null", done => {
        const jqueryStub = _ => ({trigger: x => {if(x === "filter-on"){done()}}})
        dimension.filterMulti([], null, null, jqueryStub)
      })
      it("triggers reranged if resetRange truthy", done => {
        const jqueryStub = _ => ({trigger: x => {if(x === "reranged"){done()}}})
        dimension.filterMulti([], true, null, jqueryStub)
      })
      xit("triggers filter-clear if filter changed from not null to null", done => {
        const jqueryStub = _ => ({trigger: x => {if(x === "filter-clear"){done()}}})
        dimension.filterRange([1,2])
        // TODO how to set filterNowNull to true?
        dimension.filterMulti([], null, null, jqueryStub)
      })
      it("AND concats if drillDownFilter set", () => {
        dimension.setDrillDownFilter("drilldown is true")
        const jqueryStub = _ => ({trigger: _ => _})
        expect(dimension.filterMulti([1,2], null, null, jqueryStub)).to.eql(dimension)
        expect(dimension.getFilterString()).to.eq("(bargle = 1 AND bargle = 2)")
      })
      it("OR concats if drillDownFilter not set", () => {
        const jqueryStub = _ => ({trigger: _ => _})
        expect(dimension.filterMulti([1,2], null, null, jqueryStub)).to.eql(dimension)
        expect(dimension.getFilterString()).to.eq("(bargle = 1 OR bargle = 2)")
      })
    })
    describe(".filterLike", () => {
      it("escapes apostrophes in filter values", () => {
        const jqueryStub = _ => ({trigger: _ => _})
        dimension.filterLike("McDonald's")
        expect(dimension.getFilterString()).to.eq("bargle like '%McDonald''s%'")
        dimension.filterAll(null, jqueryStub)
        dimension.filterLike("'til later")
        expect(dimension.getFilterString()).to.eq("bargle like '%''til later%'")
        dimension.filterAll(null, jqueryStub)
        dimension.filterLike("ye ol'")
        expect(dimension.getFilterString()).to.eq("bargle like '%ye ol''%'")
        dimension.filterAll(null, jqueryStub)
        dimension.filterLike("Wouldn't've")
        expect(dimension.getFilterString()).to.eq("bargle like '%Wouldn''t''ve%'")
      })
      it("escapes percent signs in filter values", () => {
        const jqueryStub = _ => ({trigger: _ => _})
        dimension.filterLike("100%")
        expect(dimension.getFilterString()).to.eq("bargle like '%100\\%%'")
        dimension.filterAll(null, jqueryStub)
        dimension.filterLike("% change")
        expect(dimension.getFilterString()).to.eq("bargle like '%\\% change%'")
        dimension.filterAll(null, jqueryStub)
        dimension.filterLike("%%")
        expect(dimension.getFilterString()).to.eq("bargle like '%\\%\\%%'")
      })
      it("escapes underscores in filter values", () => {
        const jqueryStub = _ => ({trigger: _ => _})
        dimension.filterLike("100_")
        expect(dimension.getFilterString()).to.eq("bargle like '%100\\_%'")
        dimension.filterAll(null, jqueryStub)
        dimension.filterLike("_ change")
        expect(dimension.getFilterString()).to.eq("bargle like '%\\_ change%'")
        dimension.filterAll(null, jqueryStub)
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
        expect(dimension.getFilterString()).to.eq("bargle like '%bob%'bargle like '%marl%'") // TODO this doesn't look like proper SQL
      })
    })
    describe(".filterILike", () => {
      it("escapes apostrophes in filter values", () => {
        const jqueryStub = _ => ({trigger: _ => _})
        dimension.filterILike("McDonald's")
        expect(dimension.getFilterString()).to.eq("bargle ilike '%McDonald''s%'")
        dimension.filterAll(null, jqueryStub)
        dimension.filterILike("'til later")
        expect(dimension.getFilterString()).to.eq("bargle ilike '%''til later%'")
        dimension.filterAll(null, jqueryStub)
        dimension.filterILike("ye ol'")
        expect(dimension.getFilterString()).to.eq("bargle ilike '%ye ol''%'")
        dimension.filterAll(null, jqueryStub)
        dimension.filterILike("Wouldn't've")
        expect(dimension.getFilterString()).to.eq("bargle ilike '%Wouldn''t''ve%'")
      })
      it("escapes percent signs in filter values", () => {
        const jqueryStub = _ => ({trigger: _ => _})
        dimension.filterILike("100%")
        expect(dimension.getFilterString()).to.eq("bargle ilike '%100\\%%'")
        dimension.filterAll(null, jqueryStub)
        dimension.filterILike("% change")
        expect(dimension.getFilterString()).to.eq("bargle ilike '%\\% change%'")
        dimension.filterAll(null, jqueryStub)
        dimension.filterILike("%%")
        expect(dimension.getFilterString()).to.eq("bargle ilike '%\\%\\%%'")
      })
      it("escapes underscores in filter values", () => {
        const jqueryStub = _ => ({trigger: _ => _})
        dimension.filterILike("100_")
        expect(dimension.getFilterString()).to.eq("bargle ilike '%100\\_%'")
        dimension.filterAll(null, jqueryStub)
        dimension.filterILike("_ change")
        expect(dimension.getFilterString()).to.eq("bargle ilike '%\\_ change%'")
        dimension.filterAll(null, jqueryStub)
        dimension.filterILike("__")
        expect(dimension.getFilterString()).to.eq("bargle ilike '%\\_\\_%'")
      })
      it("sets like filter if none already exists", () => {
        dimension.filterILike("bob")
        expect(dimension.getFilterString()).to.eq("bargle ilike '%bob%'")
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
    describe(".projectOnAllDimensions", () => {
      it("returns own dimension object", () => {
        expect(dimension.projectOnAllDimensions()).to.eq(dimension)
      })
      it("nulls out query if falsey", () => {
        dimension.projectOnAllDimensions(false)
        dimension.top(1, 1)
        expect(dimension.getResultSet()).to.eq(null)
      })
      it("allows query creation if truthy", () => {
        const dataConnector = {getFields: _ => [], query: _ => _}
        dimension = crossfilter.setData(dataConnector, "table1").dimension("bargle")
        dimension.projectOnAllDimensions(true)
        dimension.top(1, 1)
        expect(dimension.getResultSet()).to.eq("SELECT bargle FROM table1 ORDER BY bargle DESC LIMIT 1 OFFSET 1")
      })
    })
    describe(".getResultSet", () => {
      it("returns resultSet", () => {
        const dataConnector = {getFields: _ => [], query: _ => _}
        dimension = crossfilter.setData(dataConnector, "table1").dimension("bargle")
        expect(dimension.getResultSet()).to.eq(null)
        dimension.projectOnAllDimensions(true)
        dimension.top(1, 1)
        expect(dimension.getResultSet()).to.eq("SELECT bargle FROM table1 ORDER BY bargle DESC LIMIT 1 OFFSET 1")
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
    describe(".top", () => {
      beforeEach(() => {
        const dataConnector = {getFields: _ => [], query: _ => _}
        crossfilter = cf.crossfilter(dataConnector, "users")
        dimension = crossfilter.dimension("id")
        dimension.projectOnAllDimensions(true)
      })
      it("returns empty object if no query", () => {
        dimension.projectOnAllDimensions(false)
        expect(dimension.top()).to.eql({})
      })
      it("constructs and runs query", () => {
        expect(dimension.top(1, 2)).to.eq("SELECT id FROM users ORDER BY id DESC LIMIT 1 OFFSET 2")
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
      it("can return sync results", () => {
        const dataConnector = {getFields: _ => [], query: _ => 2}
        crossfilter = cf.crossfilter(dataConnector, "users")
        dimension = crossfilter.dimension("id")
        dimension.projectOnAllDimensions(true)
        expect(dimension.top(1)).to.eq(2)
      })
      xit("can return async results", () => {
        expect(callback).to.work
      })
      it("can include a dimension's rowid", () => {
        expect(dimension.top(Infinity, 1, true)).to.eq("SELECT id,users.rowid FROM users ORDER BY id DESC OFFSET 1") // TODO rowid squished
      })
      it("can use a samplingRatio < 1 with filterQuery", () => {
        dimension.samplingRatio(0.5)
        expect(dimension.top(Infinity)).to.eq("SELECT id FROM users WHERE  MOD(users.rowid * 265445761, 4294967296) < 2147483648 ORDER BY id DESC") // TODO extra space between WHERE & MOD
      })
      it("can use a samplingRatio < 1 without filterQuery", () => {
        dimension.filterExact(1)
        dimension.samplingRatio(0.5)
        expect(dimension.top(Infinity)).to.eq("SELECT id FROM users WHERE id = 1 AND  MOD(users.rowid * 265445761, 4294967296) < 2147483648 ORDER BY id DESC") // TODO extra space between AND & MOD
      })
      it("can use a join statement with no filterQuery or samplingRatio", () => {
        const dataConnector = {getFields: _ => []}
        const dataTables = "tableA"
        const joinAttrs = [{table1:"table1", table2:"table2", attr1:"id", attr2:"x_id"}]
        crossfilter.setData(dataConnector, dataTables, joinAttrs)
        dimension.samplingRatio(0.5)
        expect(dimension.top(Infinity)).to.eq("SELECT id FROM tableA WHERE  MOD(tableA.rowid * 265445761, 4294967296) < 2147483648 AND table1.id = table2.x_id ORDER BY id DESC") // TODO extra space between WHERE & MOD
      })
      it("can use a joinStatement with no filterQuery and samplingRatio >= 1", () => {
        const dataConnector = {getFields: _ => []}
        const dataTables = "tableA"
        const joinAttrs = [{table1:"table1", table2:"table2", attr1:"id", attr2:"x_id"}]
        crossfilter.setData(dataConnector, dataTables, joinAttrs)
        dimension.samplingRatio(1.5)
        expect(dimension.top(Infinity)).to.eq("SELECT id FROM tableA WHERE table1.id = table2.x_id ORDER BY id DESC") // TODO extra space between WHERE & MOD
      })
      it("AND concats multiple filters/dimensions", () => {
        dimension.filter([1, 2])
        dimension = crossfilter.dimension(["rx", "sex"])
        dimension.filter([3, 4])
        dimension.projectOnAllDimensions(true)
        expect(dimension.top(Infinity)).to.eq("SELECT id,rx, sex FROM users WHERE (id >= 1 AND id < 2) AND rx = 3 AND sex = 4 ORDER BY rx, sex DESC") // TODO text squished
      })
    })
    describe(".topAsync", () => {
      it("is alias for .top", () => {
        expect(dimension.topAsync).to.eq(dimension.top)
      })
    })
    describe(".bottom", () => {
      beforeEach(() => {
        const dataConnector = {getFields: _ => [], query: _ => _}
        crossfilter = cf.crossfilter(dataConnector, "users")
        dimension = crossfilter.dimension("id")
        dimension.projectOnAllDimensions(true)
      })
      it("returns empty object if no query", () => {
        dimension.projectOnAllDimensions(false)
        expect(dimension.bottom()).to.eql({})
      })
      it("constructs and runs query", () => {
        expect(dimension.bottom(1, 2)).to.eq("SELECT id FROM users ORDER BY idASC LIMIT 1 OFFSET 2") // TODO invalid sql
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
      it("can return sync results", () => {
        const dataConnector = {getFields: _ => [], query: _ => 2}
        crossfilter = cf.crossfilter(dataConnector, "users")
        dimension = crossfilter.dimension("id")
        dimension.projectOnAllDimensions(true)
        expect(dimension.bottom(1)).to.eq(2)
      })
      xit("can return async results", () => {
        expect(callback).to.work
      })
    })
    describe(".bottomAsync", () => {
      it("is alias for .top", () => {
        expect(dimension.topAsync).to.eq(dimension.top)
      })
    })
    describe(".group", () => {
      let group
      beforeEach(() => {
        crossfilter.setData({query: _ => _, getFields: () => []}, "table1")
        group = dimension.group()
      })
      xit("returns group object", () => {
        expect(dimension.group()).to.eql(group)
      })
      describe(".reduceCount", () => { // TODO duplicates crossfilter.groupAll methods
        it("returns own group object", () => {
          expect(group.reduceCount()).to.eql(group)
        })
        it("sets reduceExpression to count(*) with no arg", () => {
          group.reduceCount()
          expect(group.getReduceExpression()).to.eq("COUNT(*) AS val")
        })
        it("sets reduceExpression to based on arg", () => {
          group.reduceCount("x.id")
          expect(group.getReduceExpression()).to.eq("COUNT(x.id) AS val")
        })
      })
      describe(".reduceSum", () => { // TODO duplicates crossfilter.groupAll methods
        it("returns own group object", () => {
          expect(group.reduceSum()).to.eql(group)
        })
        xit("validates input")
        it("sets reduceExpression", () => {
          group.reduceSum("x.id")
          expect(group.getReduceExpression()).to.eq("SUM(x.id) AS val")
        })
      })
      describe(".reduceAvg", () => { // TODO duplicates crossfilter.groupAll methods
        it("returns own group object", () => {
          expect(group.reduceAvg()).to.eql(group)
        })
        xit("validates input")
        it("sets reduceExpression", () => {
          group.reduceAvg("x.id")
          expect(group.getReduceExpression()).to.eq("AVG(x.id) AS val")
        })
      })
      describe(".reduceMin", () => { // TODO duplicates crossfilter.groupAll methods
        it("returns own group object", () => {
          expect(group.reduceMin()).to.eql(group)
        })
        xit("validates input")
        it("sets reduceExpression", () => {
          group.reduceMin("x.id")
          expect(group.getReduceExpression()).to.eq("MIN(x.id) AS val")
        })
      })
      describe(".reduceMax", () => { // TODO duplicates crossfilter.groupAll methods
        it("returns own group object", () => {
          expect(group.reduceMax()).to.eql(group)
        })
        xit("validates input")
        it("sets reduceExpression", () => {
          group.reduceMax("x.id")
          expect(group.getReduceExpression()).to.eq("MAX(x.id) AS val")
        })
      })
      describe(".reduce", () => { // TODO duplicates crossfilter.groupAll methods
        it("returns own group object", () => {
          expect(group.reduce([])).to.eql(group)
        })
        xit("validates input")
        it("sets reduceExpression", () => {
          const expressions = [{expression:"x.id", agg_mode:"MIN", name:"minx"}]
          group.reduce(expressions)
          expect(group.getReduceExpression()).to.eq("MIN(x.id) AS minx")
        })
        it("sets reduceExpression when agg_mode is COUNT", () => {
          const expressions = [{expression:"x.id", agg_mode:"COUNT", name:"minx"}]
          group.reduce(expressions)
          expect(group.getReduceExpression()).to.eq("COUNT(x.id) AS minx")
        })
        it("sets reduceExpression when agg_mode is COUNT and no expression", () => {
          const expressions = [{agg_mode:"COUNT", name:"minx"}]
          group.reduce(expressions)
          expect(group.getReduceExpression()).to.eq("COUNT(*) AS minx")
        })
        it("sets reduceExpression for multiple expressions", () => {
          const expressions = [
            {expression:"x.id", agg_mode:"AVG", name:"avgx"},
            {expression:"y.rx", agg_mode:"MAX", name:"maxy"}
          ]
          group.reduce(expressions)
          expect(group.getReduceExpression()).to.eq("AVG(x.id) AS avgx,MAX(y.rx) AS maxy")
        })
        it("doesn't null out * when used in agg function", () => {
          const expressions = [{agg_mode:"COUNT", name:"count_all"}, {expression: "*", agg_mode:"COUNT", name:"count_star"}]
          group.reduce(expressions)
          expect(group.top(1)).to.eq("SELECT bargle as key0,COUNT(*) AS count_all,COUNT(*) AS count_star FROM table1 GROUP BY key0 ORDER BY count_all DESC,count_star DESC LIMIT 1")
        })
        it("sets composite reduceExpression", () => {
          const expressions = [{expression: "arrdelay+depdelay", agg_mode: "AVG", name: "avg_delays", isComposite: true}]
          group.reduce(expressions)
          expect(group.top(1)).to.eq("SELECT bargle as key0,AVG(arrdelay+depdelay) AS avg_delays FROM table1 GROUP BY key0 ORDER BY avg_delays DESC LIMIT 1")
        })
      })
      describe(".reduceMulti", () => { // TODO duplicates crossfilter.groupAll methods
        it("is an alias for .reduce", () => {
          expect(group.reduce).to.eq(group.reduceMulti)
        })
      })
      describe(".size", () => { // TODO similar to crossfilter.size
        it("returns number of records", () => {
          crossfilter.setData({getFields:() => [], query:() => [{n:123}]}, null, [])
          expect(group.size()).to.eql(123)
        })
        xit("constructs a valid query when _joinStmt is undefined")
      })
      describe(".top", () => {
        beforeEach(() => {
          const dataConnector = {getFields: _ => [], query: _ => _}
          crossfilter = cf.crossfilter(dataConnector, "users")
          dimension = crossfilter.dimension("id")
          group = dimension.group()
          dimension.projectOnAllDimensions(true)
        })
        it("constructs and runs query", () => {
          expect(group.top(1, 2)).to.eq("SELECT id as key0,COUNT(*) AS val FROM users GROUP BY key0 ORDER BY val DESC LIMIT 1 OFFSET 2")
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
            {expression:"age", agg_mode:"MAX", name:"max_age"},
            {expression:"lbs", agg_mode:"AVG", name:"avg_lbs"},
            {expression:"cty", agg_mode:"COUNT", name:"cnt_cty"}
          ])
          expect(group.top(1)).to.eq("SELECT id as key0,MAX(age) AS max_age,AVG(lbs) AS avg_lbs,COUNT(cty) AS cnt_cty FROM users WHERE age IS NOT NULL AND lbs IS NOT NULL AND cty IS NOT NULL GROUP BY key0 ORDER BY max_age DESC,avg_lbs DESC,cnt_cty DESC LIMIT 1")
        })
        it("works with reduce expressions including COUNT(*)", () => {
          group.reduce([
            {expression:"lbs", agg_mode:"AVG", name:"avg_lbs"},
            {agg_mode:"COUNT", name:"cnt_cty"},
            {expression:"*", agg_mode:"COUNT", name:"cnt_bad"}
          ])
          expect(group.top(1)).to.eq("SELECT id as key0,AVG(lbs) AS avg_lbs,COUNT(*) AS cnt_cty,COUNT(*) AS cnt_bad FROM users WHERE lbs IS NOT NULL GROUP BY key0 ORDER BY avg_lbs DESC,cnt_cty DESC,cnt_bad DESC LIMIT 1")
        })
        it("can return sync results", () => {
          const dataConnector = {getFields: _ => [], query: _ => 2}
          crossfilter = cf.crossfilter(dataConnector, "users")
          dimension = crossfilter.dimension("id")
          dimension.projectOnAllDimensions(true)
          group = dimension.group()
          expect(group.top(1)).to.eq(2)
        })
        xit("can return async results", () => {
          expect(callback).to.work
        })
      })
      describe(".topAsync", () => { // TODO duplicates dimension methods
        it("is alias for .top", () => {
          expect(group.topAsync).to.eq(group.top)
        })
      })
      describe(".bottom", () => {
        beforeEach(() => {
          const dataConnector = {getFields: _ => [], query: _ => _}
          crossfilter = cf.crossfilter(dataConnector, "users")
          dimension = crossfilter.dimension("id")
          group = dimension.group()
          dimension.projectOnAllDimensions(true)
        })
        it("constructs and runs query", () => { // TODO order not explicit
          expect(group.bottom(1, 2)).to.eq("SELECT id as key0,COUNT(*) AS val FROM users GROUP BY key0 ORDER BY val LIMIT 1 OFFSET 2")
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
            {expression:"id", agg_mode:"MIN", name:"min_id"},
            {expression:"rx", agg_mode:"SUM", name:"sum_rx"}
          ])
          expect(group.bottom(1)).to.eq("SELECT id as key0,MIN(id) AS min_id,SUM(rx) AS sum_rx FROM users WHERE id IS NOT NULL AND rx IS NOT NULL GROUP BY key0 ORDER BY min_id,sum_rx LIMIT 1")
        })
        it("can return sync results", () => {
          const dataConnector = {getFields: _ => [], query: _ => 2}
          crossfilter = cf.crossfilter(dataConnector, "users")
          dimension = crossfilter.dimension("id")
          dimension.projectOnAllDimensions(true)
          group = dimension.group()
          expect(group.bottom(1)).to.eq(2)
        })
        xit("can return async results", () => {
          expect(callback).to.work
        })
      })
      describe(".bottomAsync", () => { // TODO duplicates dimension methods
        it("is alias for .bottom", () => {
          expect(group.bottomAsync).to.eq(group.bottom)
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
      describe(".binParams", () => {
        it("returns own group object", () => {
          expect(group.binParams([])).to.eql(group)
        })
        it("returns bin params if no args", () => {
          expect(group.binParams()).to.eq(null)
          group.binParams("a")
          expect(group.binParams()).to.eql(["a"])
        })
        it("arrayifies params if necessary", () => {
          expect(group.binParams()).to.eq(null)
          group.binParams("a")
          expect(group.binParams()).to.eql(["a"])
          group.binParams(["b", "c"])
          expect(group.binParams()).to.eql(["b", "c"])
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
      describe(".setEliminateNull", () => {
        it("returns own group object", () => {
          expect(group.setEliminateNull([])).to.eql(group)
        })
        it("sets eliminateNull", () => {
          expect(group.getEliminateNull()).to.eq(true)
          group.setEliminateNull(false)
          expect(group.getEliminateNull()).to.eq(false)
          group.setEliminateNull(true)
          expect(group.getEliminateNull()).to.eq(true)
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
      xdescribe(".truncDate", () => { // TODO blows up always
        it("returns own group object", () => {
          expect(group.truncDate()).to.eq(group)
        })
        xit("sets dateTruncLevel to arg")
        xit("sets binCount to binCountIn")
      })
      describe(".actualTimeBin", () => {
        const genQueryBinParams  = (minTime, maxTime, maxBins) => [{binBounds: [{getTime: _ => minTime}, {getTime: _ => maxTime}], numBins: maxBins, timeBin: "auto"}]
        it("returns the actual time bin", () => {
          group.binParams(genQueryBinParams())
          expect(group.actualTimeBin(0)).to.eq('century')
        })
        it("uses rangeFilters if boundByFilter truthy and rangeFilters exist", () => {
          group.setBoundByFilter(true)
          const bounds = [{getTime: _ => _}, {getTime: _ => _}]
          dimension.filterRange(bounds, null, true)
          group.binParams(genQueryBinParams())
          expect(group.actualTimeBin(0)).to.eq('century')
        })
        it("works for multiple dimensions", () => {
          group = crossfilter.dimension(["id", "age"]).group()
          const bounds = [{getTime: _ => _}, {getTime: _ => _}]
          group.binParams([{binBounds: bounds, timeBin: "auto"}, {binBounds: bounds, timeBin: "auto"}])
          expect(group.actualTimeBin(0)).to.eq('century')
        })
        it("returns time spans", () => {
          group.binParams(genQueryBinParams(1, 1000, 1))
          expect(group.actualTimeBin(0)).to.eq('second')
          group.binParams(genQueryBinParams(1, 10000, 1))
          expect(group.actualTimeBin(0)).to.eq('minute')
          group.binParams(genQueryBinParams(1, 100000, 1))
          expect(group.actualTimeBin(0)).to.eq('hour')
          group.binParams(genQueryBinParams(1, 10000000, 1))
          expect(group.actualTimeBin(0)).to.eq('day')
          group.binParams(genQueryBinParams(1, 100000000, 1))
          expect(group.actualTimeBin(0)).to.eq('week')
          group.binParams(genQueryBinParams(1, 1000000000, 1))
          expect(group.actualTimeBin(0)).to.eq('month')
          group.binParams(genQueryBinParams(1, 10000000000, 1))
          expect(group.actualTimeBin(0)).to.eq('quarter')
          group.binParams(genQueryBinParams(1, 20000000000, 1))
          expect(group.actualTimeBin(0)).to.eq('year')
          group.binParams(genQueryBinParams(1, 100000000000, 1))
          expect(group.actualTimeBin(0)).to.eq('decade')
        })
        it("returns a time span to give the correct number of bins", () => {
          group.binParams(genQueryBinParams(1,10000,10))
          expect(group.actualTimeBin(0)).to.eq('second')
          group.binParams(genQueryBinParams(1,10000,1))
          expect(group.actualTimeBin(0)).to.eq('minute')
        })
        it("returns null unless queryBinParams is a non-empty array", () => {
          expect(group.actualTimeBin(2)).to.eq(null)
        })
      })
      describe(".numBins", () => {
        it("returns num bins if no arguments", () => {
          expect(group.numBins()).to.eql([])
          group.binParams([{numBins:1}, {numBins:2}])
          expect(group.numBins()).to.eql([1, 2])
        })
        it("returns own group object if args", () => {
          group.binParams([])
          expect(group.numBins([])).to.eq(group)
        })
        it("arrayifies args if necessary", () => {
          group.binParams([{}])
          group.numBins(2)
          expect(group.binParams()).to.eql([{numBins:2}])
        })
        it("overwrites binParams", () => {
          group.binParams([{}, {}])
          group.numBins([1,2])
          expect(group.binParams()).to.eql([{numBins:1}, {numBins:2}])
        })
        it("throws an error if arg length doesn't match binParams length", () => {
          group.binParams([1, 2])
          expect(group.numBins.bind(null, [1, 2, 3])).to.throw("Num bins length must be same as bin params length")
        })
      })
      describe(".all", () => {
        it("returns sync query result", () => {
          crossfilter.setData({query: _ => 123, getFields: () => []})
          group = dimension.group()
          expect(group.all()).to.eq(123)
        })
        it("creates query", () => {
          expect(group.all()).to.eq("SELECT bargle as key0,COUNT(*) AS val FROM table1 GROUP BY key0 ORDER BY key0") // TODO count squished
        })
        it("handles multiple dimensions", () => {
          group = crossfilter.dimension(["id", "age"]).group()
          expect(group.all()).to.eq("SELECT id as key0,age as key1,COUNT(*) AS val FROM table1 GROUP BY key0, key1 ORDER BY key0,key1") // TODO syntax squished
        })
        xit("unbins results if no binParams", () => {
          crossfilter.setData({getFields: () => [], getPlatform: _ => _})
          group.binParams([{binBounds:true}])
          expect(group.all()).to.eq("wip")
        })
        xit("returns async result via callback")
      })
      describe(".writeFilter", () => {
        it("returns filter statement", () => {
          dimension.filter(6)
          dimension.setDrillDownFilter(true)
          expect(group.writeFilter()).to.eq("bargle = 6")
        })
        it("AND concats filters", () => {
          dimension.filter(1)
          crossfilter.filter().filter("id = 2")
          group = crossfilter.dimension("id").group()
          expect(group.writeFilter()).to.eq("bargle = 1 AND id = 2")
        })
        it("uses args to form filter if provided", () => {
          const queryBinParams = [{binBounds: [1,2]}]
          expect(group.writeFilter(queryBinParams)).to.eq("(bargle >= 1 AND bargle < 2)")
        })
        it("AND concats from multiple dimensions to form filter", () => {
          dimension.filter(1)
          dimension = crossfilter.dimension("id")
          group = dimension.group()
          const queryBinParams = [{binBounds: [1,2]}]
          expect(group.writeFilter(queryBinParams)).to.eq("bargle = 1 AND (id >= 1 AND id < 2)")
        })
        it("uses rangeFilters if present", () => {
          dimension.filterRange([3,4], null, true)
          group.setBoundByFilter(true)
          const queryBinParams = [{binBounds: [1,2]}]
          expect(group.writeFilter(queryBinParams)).to.eq("(bargle >= 3 AND bargle < 4)")
        })
        it("returns only selfFilter if no other filters", () => {
          dimension.selfFilter("custom = 2")
          expect(group.writeFilter()).to.eq("custom = 2")
        })
        it("concats selfFilter and other filters", () => {
          dimension.selfFilter("custom = 2")
          const queryBinParams = [{binBounds: [1,2]}]
          expect(group.writeFilter(queryBinParams)).to.eq("(bargle >= 1 AND bargle < 2) AND custom = 2")
        })
        it("prevents nulls for non-COUNT agg modes", () => {
          group.reduceAvg("age")
          expect(group.top(1)).to.eq("SELECT bargle as key0,AVG(age) AS val FROM table1 WHERE age IS NOT NULL GROUP BY key0 ORDER BY val DESC LIMIT 1")
        })
        it("does null COUNT measures", () => {
          group.reduceCount("lbs")
          expect(group.top(1)).to.eq("SELECT bargle as key0,COUNT(lbs) AS val FROM table1 WHERE lbs IS NOT NULL GROUP BY key0 ORDER BY val DESC LIMIT 1")
        })
        xit("prevents no nulls when no agg mode", () => {
          group.reduce("")
          expect(group.top(1)).to.eq("SELECT bargle as key0 FROM table1 GROUP BY key0 ORDER BY val DESC LIMIT 1")
        })
      })
    })
    describe(".groupAll", () => {
      it("is an alias for crossfilter.groupAll", () => {
        expect(dimension.groupAll).to.eq(crossfilter.groupAll)
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
    describe(".setDrillDownFilter", () => {
      it("returns own dimension object", () => {
        expect(dimension.setDrillDownFilter(1)).to.eql(dimension)
      })
      it("sets drillDownFilter", () => {
        const jqueryStub = _ => ({trigger: _ => _})
        expect(dimension.filterMulti([1,2], null, null, jqueryStub)).to.eql(dimension)
        expect(dimension.getFilterString()).to.eq("(bargle = 1 OR bargle = 2)")
        dimension.setDrillDownFilter(true)
        expect(dimension.filterMulti([1,2], null, null, jqueryStub)).to.eql(dimension)
        expect(dimension.getFilterString()).to.eq("(bargle = 1 AND bargle = 2)")
        dimension.setDrillDownFilter(false)
        expect(dimension.filterMulti([1,2], null, null, jqueryStub)).to.eql(dimension)
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
    })
    describe(".reduce", () => {
      it("returns own group object", () => {
        expect(group.reduce([])).to.eql(group)
      })
      xit("validates input")
      it("sets reduceExpression", () => {
        const expressions = [{expression:"x.id", agg_mode:"MIN", name:"minx"}]
        group.reduce(expressions)
        expect(group.getReduceExpression()).to.eq("MIN(x.id) AS minx")
      })
      it("sets reduceExpression when agg_mode is COUNT", () => {
        const expressions = [{expression:"x.id", agg_mode:"COUNT", name:"minx"}]
        group.reduce(expressions)
        expect(group.getReduceExpression()).to.eq("COUNT(x.id) AS minx")
      })
      it("sets reduceExpression when agg_mode is COUNT and no expression", () => {
        const expressions = [{agg_mode:"COUNT", name:"minx"}]
        group.reduce(expressions)
        expect(group.getReduceExpression()).to.eq("COUNT(*) AS minx")
      })
      it("sets reduceExpression for multiple expressions", () => {
        const expressions = [
          {expression:"x.id", agg_mode:"AVG", name:"avgx"},
          {expression:"y.rx", agg_mode:"MAX", name:"maxy"}
        ]
        group.reduce(expressions)
        expect(group.getReduceExpression()).to.eq("AVG(x.id) AS avgx,MAX(y.rx) AS maxy")
      })
    })
    describe(".reduceMulti", () => {
      it("is an alias for .reduce", () => {
        expect(group.reduce).to.eq(group.reduceMulti)
      })
    })
    describe(".value", () => {
      it("returns value of query result", () => {
        const dataConnector = {getFields: _ => [], query: _ => [{val:2, other:1}]}
        expect(crossfilter.setData(dataConnector, "table1").groupAll().value()).to.eq(2)
      })
      it("returns query result ignoring filters", () => {
        const dataConnector = {getFields: _ => [], query: _ => [{val:2, other:1}]}
        expect(crossfilter.setData(dataConnector, "table1", []).groupAll().value(true)).to.eq(2)
      })
      it("returns query result with joins", () => {
        const dataConnector = {getFields: _ => [], query: _ => [{val:2, other:1}]}
        expect(crossfilter.setData(dataConnector, "table1", []).groupAll().value()).to.eq(2)
      })
      it("returns query result with joins and filter", () => {
        const dataConnector = {getFields: _ => [], query: _ => [{val:2, other:1}]}
        crossfilter = crossfilter.setData(dataConnector, "table1", [])
        crossfilter.filter().filter("age < 10")
        expect(crossfilter.groupAll().value()).to.eq(2)
      })
      it("returns query result with joins and multiple filters", () => {
        const dataConnector = {getFields: _ => [], query: _ => [{val:2, other:1}]}
        crossfilter = crossfilter.setData(dataConnector, "table1", [])
        crossfilter.filter().filter("age < 10")
        crossfilter.filter().filter("id = 456")
        expect(crossfilter.groupAll().value()).to.eq(2)
      })
    })
    xdescribe(".valueAsync", () => {
      it("executes callbacks with value of query result", done => {
        const testCallback = x => {
          if( x === 2 ){ done() }
        }
        const dataConnector = {getFields: _ => [], query: _ => [{val:2, other:1}]}
        crossfilter.setData(dataConnector, "table1").groupAll().valueAsync([testCallback])
      })
    })
    describe(".values", () => {
      it("returns full query result", () => {
        const dataConnector = {getFields: _ => [], query: _ => [{val:2, other:1}]}
        expect(crossfilter.setData(dataConnector, "table1").groupAll().values()).to.eql({val:2, other:1})
      })
    })
  })
  describe(".size", () => {
    it("returns number of records", () => {
      crossfilter.setData({getFields:() => [], query:() => [{n:123}]}, null, [])
      expect(crossfilter.size()).to.eql(123)
    })
    xit("constructs a valid query when _joinStmt is undefined")
  })
  describe(".getFilter", () => {
    it("returns unset filters", () => {
      expect(crossfilter.getFilter()).to.eql([])
    })
    xit("returns set filters", () => {
      crossfilter.setData({getFields:() => []}, null, [])
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
  })
  describe(".getDimensions", () => {
    it("returns unset dimensions", () => {
      expect(crossfilter.getDimensions()).to.eql([])
    })
    it("returns set dimensions", () => {
      crossfilter.setData({getFields:() => []}, null, [])
      crossfilter.dimension("a dimension")
      expect(crossfilter.getDimensions()).to.eql(["a dimension"])
    })
  })
  describe(".getTable", () => {
    it("returns unset data tables", () => {
      expect(crossfilter.getTable()).to.eq(null)
    })
    it("returns set data tables", () => {
      crossfilter.setData({getFields:() => []}, "a table", [])
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
      resultCache.setDataConnector({query: () => 123})
      expect(resultCache.query("select *;")).to.eq(123)
    })
    it("hits cache if possible", () => {
      resultCache.setDataConnector({query: () => 123})
      expect(resultCache.query("select *;")).to.eq(123)
      resultCache.setDataConnector({query: () => 45})
      expect(resultCache.query("select *;")).to.eq(123)
      expect(resultCache.query("select (*);")).to.eq(45)
    })
    it("evicts oldest cache entry if necessary", () => {
      resultCache.setDataConnector({query: n => n})
      resultCache.setMaxCacheSize(2)
      resultCache.query("1")
      resultCache.query("2")
      expect(resultCache.peekAtCache()).to.eql({1:{time:0,data:"1"}, 2:{time:1,data:"2"}})
      resultCache.query("3")
      expect(resultCache.peekAtCache()).to.eql({2:{time:1,data:"2"}, 3:{time:2,data:"3"}})
    })
    it("post-processes data if necessary", () => {
      resultCache.setDataConnector({query: () => 2})
      const options = {
        postProcessors: [x => x + 1, x => x * 2]
      }
      expect(resultCache.query("select *;", options)).to.eq(6)
    })
    it("does not check cache if renderSpec true", () => {
      const options = {renderSpec: true}
      resultCache.setDataConnector({query: n => n})
      resultCache.query("1")
      expect(resultCache.peekAtCache()).to.eql({1:{time:0,data:"1"}})
      resultCache.setDataConnector({query: n => 10*n})
      expect(resultCache.query("1", options)).to.eql(10)
    })
    it("does not store in cache if renderSpec true", () => {
      const options = {renderSpec: true}
      resultCache.setDataConnector({query: n => n})
      resultCache.query("1")
      expect(resultCache.peekAtCache()).to.eql({1:{time:0,data:"1"}})
      resultCache.setDataConnector({query: n => 10*n})
      resultCache.query("1", options)
      expect(resultCache.peekAtCache()).to.eql({1:{time:0,data:"1"}})
    })
    it("does not evict from cache if renderSpec true", () => {
      const options = {renderSpec: true}
      resultCache.setDataConnector({query: n => n})
      resultCache.setMaxCacheSize(2)
      resultCache.query("1")
      resultCache.query("2")
      resultCache.query("3", options)
      expect(resultCache.peekAtCache()).to.eql({1:{time:0,data:"1"}, 2:{time:1,data:"2"}})
    })
  })
  describe(".queryAsync", () => {
    it("returns the data asynchronously", done => {
      resultCache.setDataConnector({query: (qry, opt, cbs) => cbs.forEach(cb => cb())})
      resultCache.queryAsync("select *", {}, [done])
    })
    it("registers and calls multiple callbacks with query result", () => {
      let counter = 0
      const incrementCounter = _ => counter += 1
      resultCache.setDataConnector({query: (qry, opt, cbs) => cbs.forEach(cb => cb())})
      resultCache.queryAsync("a", {}, [incrementCounter, incrementCounter])
      expect(counter).to.eq(2)
    })
    it("hits cache if possible", () => { // if renderSpec is falsey
      resultCache.setDataConnector({query: (qry, opt, cbs) => cbs.forEach(cb => cb(123))})
      resultCache.queryAsync("a", {}, [])
      expect(resultCache.peekAtCache()).to.eql({a:{time:0, data:123}})
      resultCache.queryAsync("a", {}, [])
      expect(resultCache.peekAtCache()).to.eql({a:{time:2, data:123}}) // TODO why is time skipping 1?
    })
    it("evicts oldest cache entry if necessary", () => {
      resultCache.setMaxCacheSize(2)
      resultCache.setDataConnector({query: (qry, opt, cbs) => cbs.forEach(cb => cb(qry))})
      resultCache.queryAsync("a", {}, [])
      resultCache.queryAsync("b", {}, [])
      expect(resultCache.peekAtCache()).to.eql({a:{time:0,data:"a"}, b:{time:1,data:"b"}})
      resultCache.queryAsync("a", {}, [])
      resultCache.queryAsync("c", {}, [])
      expect(resultCache.peekAtCache()).to.eql({a:{time:3,data:"a"}, c:{time:4,data:"c"}}) // TODO why is time skipping 2?
    })
    it("post-processes data if necessary", () => {
      const callback = x => x//{
        // TODO callback not being called with value after postProcessors
        // if(x===5){ done()}
      // }
      resultCache.setDataConnector({query: (qry, opt, cbs) => cbs.forEach(cb => cb(1))})
      const options = {
        postProcessors: [x => x * 2, x => x + 3]
      }
      resultCache.queryAsync("a", options, [callback])
      expect(resultCache.peekAtCache()).to.eql({a:{time:0,data:5}})
    })
    xit("does not check cache if renderSpec true", () => {
      const options = {renderSpec: true}
      resultCache.setDataConnector({query: n => n})
      resultCache.queryAsync("1")
      expect(resultCache.peekAtCache()).to.eql({1:{time:0,data:"1"}})
      resultCache.setDataConnector({query: n => 10*n})
      expect(resultCache.queryAsync("1", options)).to.eql(10)
    })
    xit("does not store in cache if renderSpec true", () => {
      const options = {renderSpec: true}
      resultCache.setDataConnector({query: n => n})
      resultCache.queryAsync("1")
      expect(resultCache.peekAtCache()).to.eql({1:{time:0,data:"1"}})
      resultCache.setDataConnector({query: n => 10*n})
      resultCache.queryAsync("1", options)
      expect(resultCache.peekAtCache()).to.eql({1:{time:0,data:"1"}})
    })
    xit("does not evict from cache if renderSpec true", () => {
      const options = {renderSpec: true}
      resultCache.setDataConnector({query: n => n})
      resultCache.setMaxCacheSize(2)
      resultCache.queryAsync("1")
      resultCache.queryAsync("2")
      resultCache.queryAsync("3", options)
      expect(resultCache.peekAtCache()).to.eql({1:{time:0,data:"1"}, 2:{time:1,data:"2"}})
    })
  })
  describe(".emptyCache", () => {
    it("returns itself with an empty cache", () => {
      resultCache.setDataConnector({query: () => 1})
      resultCache.query("a")
      expect(resultCache.peekAtCache()).to.eql({a:{time:0, data:1}})
      resultCache.emptyCache()
      expect(resultCache.peekAtCache()).to.eql({})
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
      resultCache.setDataConnector({the:1})
      expect(resultCache.getDataConnector()).to.eql({the:1})
    })
  })
})
describe("filterNullMeasures", () => {
  it("filters out a null measure", () => {
    const columns = [
      {expression: "id", agg_mode: "SUM"}
    ]
    expect(cf.filterNullMeasures("", columns)).to.eq("id IS NOT NULL")
  })
  it("filters out multiple null measures", () => {
    const columns = [
      {expression: "id", agg_mode: "min"},
      {expression: "age", agg_mode: "avg"}
    ]
    expect(cf.filterNullMeasures("", columns)).to.eq("id IS NOT NULL AND age IS NOT NULL")
  })
  it("filters out null measures with existing filter statement", () => {
    const columns = [
      {expression: "id", agg_mode: "count"},
      {expression: "age", agg_mode: "avg"}
    ]
    expect(cf.filterNullMeasures("1 < 2", columns)).to.eq("1 < 2 AND id IS NOT NULL AND age IS NOT NULL")
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
    expect(cf.notEmpty({a:1})).to.eq(true)
    expect(cf.notEmpty({length:0})).to.eq(true)
    expect(cf.notEmpty({length:false})).to.eq(true)
  })
  it("considers date not empty", () => {
    expect(cf.notEmpty(new Date())).to.eq(true)
  })
  it("considers symbol not empty", () => {
    expect(cf.notEmpty(Symbol())).to.eq(true)
  })
  it("considers function not empty", () => {
    expect(cf.notEmpty(function x () {})).to.eq(true)
  })
})

describe("Parse parenthesis() for custom expressions", () => {
  it("will input a string and return an array", () => {
    expect(cf.parseParensIfExist('')).to.deep.eq([''])
  })

  it("will not parse if there are no parans", () => {
    expect(cf.parseParensIfExist('flights')).to.deep.eq(['flights'])
  })

  it("will parse if there are parans", () => {
    expect(cf.parseParensIfExist('avg(flights)')).to.deep.eq(['flights'])
  })

  it("will only parse the outer layer of parans", () => {
    expect(cf.parseParensIfExist('avg(flights - avg(arrdelay))')).to.deep.eq(['flights - avg(arrdelay)'])
  })
})
