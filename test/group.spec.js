// SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import chai, { expect } from "chai"
import { sizeAsyncWithEffects, sizeSyncWithEffects } from "../src/modules/group"
import spies from "chai-spies"
chai.use(spies)
const noop = () => {}

describe("group module", () => {
  describe("sizeAsyncWithEffects", () => {
    it("should return a function", () => {
      expect(typeof sizeAsyncWithEffects()).to.equal("function")
    })
    describe("when multiDim", () => {
      describe("and when result is error", () => {
        it("should call callback with error", () => {})
      })
      describe("and when result is success", () => {
        it("should call callback with result", () => {})
      })
    })
    describe("when not multiDim", () => {
      describe("when result is error", () => {
        it("should call callback with error", () => {})
      })
      describe("when result is success", () => {
        it("should call callback with result", () => {})
      })
    })
  })
  describe("sizeSyncWithEffects", () => {
    it("should return a function", () => {
      expect(typeof sizeSyncWithEffects()).to.equal("function")
    })
    describe("when multiDim", () => {})
    describe("when not multiDim", () => {})
  })
})
