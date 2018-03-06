import chai, { expect } from "chai"
import { createQueryTask, runQueryTask } from "../src/modules/task"
import spies from "chai-spies"
const noop = () => {}
chai.use(spies)

describe("task module", () => {
  describe("createQueryTask function", () => {
    const query = "test"
    const options = { queryId: -1 }
    const method = chai.spy()
    const thunk = createQueryTask(method, query, options)
    const callback = noop

    it("should return a function", () => {
      expect(typeof thunk).to.equal("function")
    })

    it("should run the passed in method with the callback and query", () => {
      thunk(callback)
      expect(method).to.have.been.called.with(query, options, callback)
    })
  })

  describe("runQueryTask function", () => {
    describe("with callback", () => {
      it("should run task with callback as argument", () => {
        const task = chai.spy()
        const callback = noop
        runQueryTask(task, callback)
        expect(task).to.have.been.called.with(callback)
      })
    })
    describe("without callback", () => {
      it("should run the task and reutrn the result", () => {
        const result = "test"
        expect(runQueryTask(() => result)).to.equal(result)
      })

      it("should throw an error if the task errors out", () => {
        const error = "Error"
        try {
          runQueryTask(() => {
            throw error
          })
        } catch (e) {
          expect(e).to.equal(error)
        }
      })
    })
  })
})
