"use strict"
const expect = require("chai").expect
const cf = require("./mapd-crossfilter")

describe("createDateAsUTC", () => {
  xit("handles type errors", () => {
    expect(cf.createDateAsUTC("invalid")).to.eq("invalid time")
  })
  it("converts a date to UTC", () => {
    const localDate = new Date(2016,11,31,23,59,59,400) // local time
    expect(cf.createDateAsUTC(localDate).getTime()).to.eq(1483228799000) // TODO no milliseconds?
  })
})
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
  it("can setData and return self by being invoked", () => {
    const dataConnector = {getFields:_ => []}
    const dataTables = "a table"
    const crsfltr = cf.crossfilter(dataConnector, dataTables)
    expect(crsfltr.type).to.eql("crossfilter")
    expect(crsfltr.getTable()).to.eql(["a table"])
  })
  it("selects from multiple tables", () => {
    const dataConnector = {getFields:_ => [], query:_ => _}
    const dataTables = ["tableA", "tableB"]
    crossfilter.setData(dataConnector, dataTables)
    crossfilter.size()
    expect(crossfilter.peekAtCache()).to.have.key("SELECT COUNT(*) as n FROM tableA,tableB")
  })
  it("joins tables", () => {
    const dataConnector = {getFields:_ => [], query:_ => _}
    const dataTables = "table1"
    const joinAttrs = [{table1:"table1", table2:"table2", attr1:"id", attr2:"x_id"}]
    crossfilter.setData(dataConnector, dataTables, joinAttrs)
    crossfilter.size()
    expect(crossfilter.peekAtCache()).to.have.key("SELECT COUNT(*) as n FROM table1 WHERE table1.id = table2.x_id")
  })
  it("joins multiple tables", () => {
    const dataConnector = {getFields:_ => [], query:_ => _}
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
    const dataConnector = {getFields:_ => [], query:_ => _}
    const dataTables = "table1"
    const joinAttrs = [
      {table1:"table2", table2:"table1", attr1:"x_id", attr2:"id"},
      {table1:"table1", table2:"table2", attr1:"id", attr2:"x_id"}
    ]
    crossfilter.setData(dataConnector, dataTables, joinAttrs)
    crossfilter.size()
    expect(crossfilter.peekAtCache()).to.have.key("SELECT COUNT(*) as n FROM table1 WHERE table1.id = table2.x_id AND table2.id = table3.y_id")
  })
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
  describe(".setData", () => {})
  describe(".filter", () => {})
  describe(".getColumns", () => {})
  describe(".dimension", () => {})
  describe(".groupAll", () => {})
  describe(".size", () => {
    it("returns number of records", () => {
      crossfilter.setData({getFields:() => [], query:() => [{n:123}]}, null, [])
      expect(crossfilter.size()).to.eql(123)
    })
    xit("disregards any filters", () => {
      crossfilter.setData({getFields:() => [], query:() => [{n:123}]}, null, [])
      expect(crossfilter.size()).to.eql(123)
    })
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
  describe(".getFilterString", () => {})
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
        // console.log(x)
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
