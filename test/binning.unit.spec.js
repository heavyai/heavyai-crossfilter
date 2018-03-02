import chai, { expect } from "chai"
import {
  formatDateResult,
  formatExtractResult,
  unBinResults
} from "../src/modules/binning"

function unBinTime(range) {
  return range.map(date => ({
    alias: date,
    value: date
  }))
}

describe("Binning Module", () => {
  describe("unBinResults", () => {
    it("skips over null queryBinParams", () => {
      const binParams = [
        { numBins: 12, binBounds: [0, 1350] },
        null,
        { numBins: 7, binBounds: [2, 19] }
      ]
      const results = [
        { key0: 0, val: 4507041 },
        { key0: 1, val: 1875013 },
        { key0: 2, val: 425687 }
      ]
      expect(unBinResults(binParams, results)).to.deep.equal([
        { key0: [0, 112.5], key2: [NaN, NaN], val: 4507041 },
        { key0: [112.5, 225], key2: [NaN, NaN], val: 1875013 },
        { key0: [225, 337.5], key2: [NaN, NaN], val: 425687 }
      ])
    })

    it("returns list of [min] for date queryBounds when results are date-times", () => {
      const binParams = [
        {
          numBins: 4,
          binBounds: [new Date("1/1/16"), new Date("1/4/16")]
        }
      ]
      const results = [
        { key0: new Date("1/1/16"), val: 4507041 },
        { key0: new Date("1/2/16"), val: 1875013 },
        { key0: new Date("1/3/16"), val: 425687 }
      ]
      const unbinnedResults = unBinResults(binParams, results)
      const aliases = unbinnedResults.map(({ key0 }) => key0[0].alias)
      const values = unbinnedResults.map(({ key0 }) => key0[0].value)
      const vals = unbinnedResults.map(({ val }) => val)

      expect(values).to.deep.equal([
        new Date("1/1/16"),
        new Date("1/2/16"),
        new Date("1/3/16")
      ])

      expect(vals).to.deep.equal([4507041, 1875013, 425687])
    })
    it("returns list of [min, max] for date queryBounds when results are numerical", () => {
      const binParams = [
        {
          numBins: 25,
          binBounds: [new Date("1/1/16"), new Date("1/4/16")],
          timeBin: "day"
        }
      ]
      const results = [
        { key0: 0, val: 4507041 },
        { key0: 1, val: 1875013 },
        { key0: 2, val: 425687 }
      ]
      const unbinnedResults = unBinResults(binParams, results)
      const aliases = unbinnedResults.map(({ key0 }) => key0[0].alias)
      const values = unbinnedResults.map(({ key0 }) => key0[0].value)
      const vals = unbinnedResults.map(({ val }) => val)

      expect(values).to.deep.equal([
        new Date("1/1/16"),
        new Date("1/2/16"),
        new Date("1/3/16")
      ])

      expect(vals).to.deep.equal([4507041, 1875013, 425687])
    })

    it("returns list of [min, max] for non-date queryBounds", () => {
      const binParams = [
        {
          numBins: 12,
          binBounds: [0, 1350],
          timeBin: null
        }
      ]
      const results = [
        {
          key0: 0,
          val: 4507041
        },
        {
          key0: 1,
          val: 1875013
        },
        {
          key0: 2,
          val: 425687
        },
        {
          key0: 3,
          val: 41650
        },
        {
          key0: 4,
          val: 5021
        },
        {
          key0: 5,
          val: 608
        },
        {
          key0: 6,
          val: 3
        },
        {
          key0: 7,
          val: 2
        },
        {
          key0: 9,
          val: 1
        },
        {
          key0: 10,
          val: 1
        },
        {
          key0: 8,
          val: 1
        }
      ]
      expect(unBinResults(binParams, results)).to.deep.equal([
        {
          key0: [0, 112.5],
          val: 4507041
        },
        {
          key0: [112.5, 225],
          val: 1875013
        },
        {
          key0: [225, 337.5],
          val: 425687
        },
        {
          key0: [337.5, 450],
          val: 41650
        },
        {
          key0: [450, 562.5],
          val: 5021
        },
        {
          key0: [562.5, 675],
          val: 608
        },
        {
          key0: [675, 787.5],
          val: 3
        },
        {
          key0: [787.5, 900],
          val: 2
        },
        {
          key0: [1012.5, 1125],
          val: 1
        },
        {
          key0: [1125, 1237.5],
          val: 1
        },
        {
          key0: [900, 1012.5],
          val: 1
        }
      ])
    })
    it("should handle when bin param is extract", () => {
      const binParams = [
        {
          numBins: 4,
          binBounds: [new Date("1/1/16"), new Date("1/4/16")],
          extract: true,
          timeBin: "day"
        }
      ]
      const results = [
        { key0: 1, val: 4507041 },
        { key0: 2, val: 1875013 },
        { key0: 3, val: 425687 }
      ]
      expect(unBinResults(binParams, results).map(a => a.key0)).to.deep.equal([
        [{ value: 1, extractUnit: "day", isExtract: true, timeBin: "day" }],
        [{ value: 2, extractUnit: "day", isExtract: true, timeBin: "day" }],
        [{ value: 3, extractUnit: "day", isExtract: true, timeBin: "day" }]
      ])
    })
  })
})
