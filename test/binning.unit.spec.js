import chai, {expect} from 'chai'
import {formatDateResult, unBinResults} from '../src/modules/binning'

function unBinTime (range) {
  return range.map(date => ({
    alias: date,
    value: date
  }))
}

describe('Binning Module', () => {
  describe('formatDateResult', () => {
    it('should handle month', () => {
      expect(formatDateResult(new Date('7/5/2016'), "month")).to.equal("July 2016")
      expect(formatDateResult(new Date('12/5/2011'), "month")).to.equal("December 2011")
    })
    it('should handle quarters', () => {
      expect(formatDateResult(new Date('1/5/2016'), "quarter")).to.equal("Q1 2016")
      expect(formatDateResult(new Date('12/5/2011'), "quarter")).to.equal("Q4 2011")
    })
    it('should handle decades', () => {
      expect(formatDateResult(new Date('1/5/1954'), "decade")).to.equal("1950s")
      expect(formatDateResult(new Date('12/5/1987'), "decade")).to.equal("1980s")
    })
    it('should handle century', () => {
      expect(formatDateResult(new Date('1/5/1954'), "century")).to.equal("20th Century")
      expect(formatDateResult(new Date('12/5/1492'), "century")).to.equal("15th Century")
    })
  })


  describe("unBinResults", () => {
    it("skips over null queryBinParams", () => {
      const binParams = [
        {numBins: 12, binBounds: [0, 1350]},
        null,
        {numBins: 7, binBounds: [2, 19]}
      ]
      const results = [
        {key0: 0, val: 4507041},
        {key0: 1, val: 1875013},
        {key0: 2, val: 425687}
      ]
      expect(unBinResults(binParams, results)).to.eql([
        {key0: [0, 112.5], key2: [NaN, NaN], val: 4507041},
        {key0: [112.5, 225], key2: [NaN, NaN], val: 1875013},
        {key0: [225, 337.5], key2: [NaN, NaN], val: 425687}
      ])
    })

    it("returns list of [min, max] for date queryBounds when results are date-times", () => {
      const binParams = [{
        numBins: 4,
        binBounds: [new Date("1/1/16"), new Date("1/4/16")]
      }]
      const results = [
        {key0: new Date("1/1/16"), val: 4507041},
        {key0: new Date("1/2/16"), val: 1875013},
        {key0: new Date("1/3/16"), val: 425687}
      ]
      expect(unBinResults(binParams, results)).to.eql([
        {key0: unBinTime([new Date("1/1/16"), new Date("1/2/16")]), val: 4507041},
        {key0: unBinTime([new Date("1/2/16"), new Date("1/3/16")]), val: 1875013},
        {key0: unBinTime([new Date("1/3/16"), new Date("1/4/16")]), val: 425687}
      ])
    })

    it("returns list of [min, max] for date queryBounds when results are numerical", () => {
      const binParams = [{
        numBins: 12,
        binBounds: [new Date("1/1/16"), new Date("1/2/16")]
      }]
      const results = [
        {key0: 0, val: 4507041},
        {key0: 1, val: 1875013},
        {key0: 2, val: 425687}
      ]
      expect(unBinResults(binParams, results)).to.eql([
        {key0: unBinTime([new Date("1/1/16"), new Date("1/2/16")]), val: 4507041},
        {key0: unBinTime([new Date("1/2/16"), new Date("1/3/16")]), val: 1875013},
        {key0: unBinTime([new Date("1/3/16"), new Date("1/4/16")]), val: 425687}
      ])
    })

    it("returns list of [min, max] for non-date queryBounds", () => {
      const binParams = [
        {
          "numBins": 12,
          "binBounds": [
            0,
            1350
          ],
          "timeBin": null
        }
      ]
      const results = [
        {
          "key0": 0,
          "val": 4507041
        },
        {
          "key0": 1,
          "val": 1875013
        },
        {
          "key0": 2,
          "val": 425687
        },
        {
          "key0": 3,
          "val": 41650
        },
        {
          "key0": 4,
          "val": 5021
        },
        {
          "key0": 5,
          "val": 608
        },
        {
          "key0": 6,
          "val": 3
        },
        {
          "key0": 7,
          "val": 2
        },
        {
          "key0": 9,
          "val": 1
        },
        {
          "key0": 10,
          "val": 1
        },
        {
          "key0": 8,
          "val": 1
        }
      ]
      expect(unBinResults(binParams, results)).to.eql([
        {
          "key0": [
            0,
            112.5
          ],
          "val": 4507041
        },
        {
          "key0": [
            112.5,
            225
          ],
          "val": 1875013
        },
        {
          "key0": [
            225,
            337.5
          ],
          "val": 425687
        },
        {
          "key0": [
            337.5,
            450
          ],
          "val": 41650
        },
        {
          "key0": [
            450,
            562.5
          ],
          "val": 5021
        },
        {
          "key0": [
            562.5,
            675
          ],
          "val": 608
        },
        {
          "key0": [
            675,
            787.5
          ],
          "val": 3
        },
        {
          "key0": [
            787.5,
            900
          ],
          "val": 2
        },
        {
          "key0": [
            1012.5,
            1125
          ],
          "val": 1
        },
        {
          "key0": [
            1125,
            1237.5
          ],
          "val": 1
        },
        {
          "key0": [
            900,
            1012.5
          ],
          "val": 1
        }
      ])
    })
  })
})
