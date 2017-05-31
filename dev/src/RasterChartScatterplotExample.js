import { debounce } from 'lodash'

export default function RasterChartScatterplotExample(d3, dc, Crossfilter) {

    new MapdCon()
        .protocol("http")
        .host("forge.mapd.com")
        .port("9092")
        .dbName("mapd")
        .user("mapd")
        .password("HyperInteractive")
        .connect(function(error, con) {
            // Tables for the first layer of the pointmap.
            // This layer will be polygons of zipcodes and
            // will be colored by data joined from the contributions
            // table
            const tableName1 = ["contributions_donotmodify", "zipcodes"]
            const table1Joins = [{
                table1: "contributions_donotmodify",
                attr1: "contributor_zipcode",
                table2: "zipcodes",
                attr2: "ZCTA5CE10"
            }];
            // Table to use for the 2nd layer, which will be points
            // from a tweets table.
            const tableName2 = 'tweets_nov_feb'

            // Table to use for the 3nd layer, which will be points
            // from the contributions table.
            const tableName3 = 'contributions_donotmodify'

            // make 3 crossfilters for all 3 layers
            // A CrossFilter instance is used for generating the raw query strings for your MapdCon.
            // debugger
            // first layer
            Crossfilter.crossfilter(con, tableName1, table1Joins)
                .then(function(polycfLayer1) {

                    // second layer
                    Crossfilter.crossfilter(con, tableName2)
                        .then(function(pointcfLayer2) {

                            // third layer
                            Crossfilter.crossfilter(con, tableName3)
                                .then(function(pointcfLayer3) {
                                    createPointMap(polycfLayer1, pointcfLayer2, pointcfLayer3, con)
                                })
                        })
                })
        })

    function createPointMap(polycfLayer1, pointcfLayer2, pointcfLayer3, con) {
        let w = document.documentElement.clientWidth - 30,
            h = Math.max(document.documentElement.clientHeight, window.innerHeight || 0) - 200

        const countGroup1 = pointcfLayer2.groupAll(),
            dataCount1 = dc.countWidget(".data-count1")
                .dimension(pointcfLayer2)
                .group(countGroup1),
            countGroup2 = pointcfLayer3.groupAll(),
            dataCount2 = dc.countWidget(".data-count2")
                .dimension(pointcfLayer3)
                .group(countGroup2)

        /*----------------BUILD THE LAYERS OF THE POINTMAP-------------------------*/
        /**-----BUILD LAYER #1, POLYGONS OF ZIPCODES COLORED BY AVG CONTRIBUTION----*/
        const polyDim1 = polycfLayer1.dimension("zipcodes.rowid"),
            // we're going to color based on the average contribution of the zipcode, so reduce the average from the join
            polyGrp1 = polyDim1.group().reduceAvg("contributions_donotmodify.amount", "avgContrib"),
            polyColorRange = ["#115f9a","#1984c5","#22a7f0","#48b5c4","#76c68f","#a6d75b","#c9e52f","#d0ee11","#d0f400"],
            polyFillColorScale = d3.scale.quantize().domain([0, 5000]).range(polyColorRange)

        // setup the first layer, the zipcode polygons
        const polyLayer1 = dc.rasterLayer("polys")
            .dimension(polyDim1)
            .group(polyGrp1)
            // .cap(100)  // We can add a cap if we want.
            .fillColorScale(polyFillColorScale)  // set the fill color scale
            .fillColorAttr('avgContrib')   // set the driving attribute for the fill color scale, in
            // this case, the average contribution
            .defaultFillColor("green")     // Set a default fill color.
            // .defaultStrokeColor("red")  // can optionally set up stroking of the polys to see
            // .defaultStrokeWidth(4)
            .popupColumns(['avgContrib', 'ZCTA5CE10']) // setup the columns we want to show when
            // hit-testing the polygons
            .popupColumnsMapped({avgContrib: "avg contribution", ZCTA5CE10: 'zipcode'})
        // setup a map so rename the popup columns
        // to something readable.
        // .popupStyle({                 // can optionally setup a different style for the popup
        //     fillColor: "transparent"  // geometry. By default, the popup geom is colored the
        // })                            // same as the fill/stroke color attributes
        /**-----------BUILD LAYER #2, POINTS OF TWEETS-------------*/
        /*-----SIZED BY # OF FOLLOWERS AND COLORED BY LANGUAGE----*/
        const pointMapDim2 = pointcfLayer2.dimension(null).projectOn(["goog_x", "goog_y", "lang as color", "followers as size"]),
            xDim2 = pointcfLayer2.dimension("goog_x"),
            yDim2 = pointcfLayer2.dimension("goog_y"),
            sizeScaleLayer2 = d3.scale.linear().domain([0,5000]).range([2,12]).clamp(true),
            langDomain = ['en', 'pt', 'es', 'in', 'und', 'ja', 'tr', 'fr', 'tl', 'ru', 'ar'],
            langColors = ["#27aeef", "#ea5545", "#87bc45", "#b33dc6", "#f46a9b", "#ede15b", "#bdcf32", "#ef9b20", "#4db6ac", "#edbf33", "#7c4dff"],
            layer2ColorScale = d3.scale.ordinal().domain(langDomain).range(langColors);

        // setup the second layer, points of the tweets.
        const pointLayer2 = dc.rasterLayer("points")
            .dimension(pointMapDim2)  // need a dimension and a group, but just supply
            .group(pointMapDim2)      // the dimension as the group too as we're not grouping anything
            .xDim(xDim2)              // add the x dimension
            .yDim(yDim2)              // add the y dimension
            .xAttr("goog_x")               // indicate project column that'll drive the x dimension
            .yAttr("goog_y")               // indicate project column that'll drive the y dimension
            .sizeScale(sizeScaleLayer2)      // setup the scale used to adjust the size of the points
            .sizeAttr("size")       // indicate which column will drive the size scale
            .fillColorScale(layer2ColorScale) // set the scale to use to define the fill color
            // of the points
            .fillColorAttr("color")   // indicate which column will drive the fill color scale
            .defaultFillColor("#80DEEA") // set a default color for cases where the language
            // of a tweet is not found in the domain fo the scale
            .cap(500000)              // set a max number of points to render. This is required
            // for point layers.
            .sampling(true)           // set sampling so you get a more equal distribution
            // of the points.
            .popupColumns(['tweet_text', 'sender_name', 'tweet_time', 'lang', 'origin', 'followers'])
        // setup the columns to show when a point is properly hit-tested
        // against
        /**---------------BUILD LAYER #3, POINTS OF CONTRIBUTIONS-------------------*/
        /*--------COLORED BY THE CONTRIBUTION RECIPIENT'S PARTY AFFILIATON---------*/
        /*--AND WHOSE SIZE IS DYNAMICALLY CONTROLLED BASED ON NUMBER OF PTS DRAWN--*/
        const pointMapDim3 = pointcfLayer3.dimension(null).projectOn(["merc_x", "merc_y", "recipient_party as color"]),
            xDim3 = pointcfLayer3.dimension("merc_x"),
            yDim3 = pointcfLayer3.dimension("merc_y"),
            dynamicSizeScale = d3.scale.sqrt().domain([100000,0]).range([1.0,7.0]).clamp(true),
            layer3ColorScale = d3.scale.ordinal().domain(["D", "R"]).range(["blue", "red"])

        const pointLayer3 = dc.rasterLayer("points")
            .dimension(pointMapDim3)  // need a dimension and a group, but just supply
            .group(pointMapDim3)      // the dimension as the group too as we're not grouping anything
            .xDim(xDim3)              // add the x dimension
            .yDim(yDim3)              // add the y dimension
            .xAttr("merc_x")               // indicate which column will drive the x dimension
            .yAttr("merc_y")               // indicate which column will drive the y dimension
            .fillColorScale(layer3ColorScale) // set the scale to use to define the fill color
            // of the points
            .fillColorAttr("color")   // indicate which column will drive the fill color scale
            .defaultFillColor("green") // set a default color so points that aren't democrat or
            // republican get a color
            .defaultSize(1)         // set a default size for the points
            .dynamicSize(dynamicSizeScale)  // but setup dynamic sizing of the points according
            // to the number of points drawn
            .cap(500000)              // set a cap for the # of points to draw, this is required
            // for point layers
            .sampling(true)           // activate sampling so the points rendered are evenly
            // distributed
            .popupColumns(['amount', 'recipient_party', 'recipient_name'])  // setup columns to show when a point is properly hit-tested

        /**---------------BUILD THE SCATTERPLOT-------------*/
        // grab the parent div.
        const parent = document.getElementById("raster-chart-container")

        const pointMapChart = dc.rasterChart(parent, true)
            .con(con)             // indicate the connection layer
            .usePixelRatio(true)  // tells the widget to use the pixel ratio of the
            // screen for proper sizing of the backend-rendered image
            .useLonLat(true)    // all point layers need their x,y coordinates, which
            // are lon,lat converted to mercator.
            .height(h/1.5)  // set width/height
            .width(w)
            .mapUpdateInterval(750)
            .mapStyle('mapbox://styles/mapbox/light-v8') // this is the default

            // add the layers to the pointmap
            .pushLayer('polytable1', polyLayer1)
            .pushLayer('pointtable1', pointLayer2)
            .pushLayer('pointtable2', pointLayer3)
            // and setup a buffer radius around the pixels for hit-testing
            // This radius helps to properly resolve hit-testing at boundaries
            .popupSearchRadius(2)

        pointMapChart.init().then(() => {
            // now render the pointmap
            dc.renderAllAsync()
            /*---------------SETUP HIT-TESTING-------------*/
            // hover effect with popup
            // Use a flag to determine if the map is in motion
            // or not (pan/zoom/etc)
            let mapmove = false
            // debounce the popup - we only want to show the popup when the
            // cursor is idle for a portion of a second.
            const debouncedPopup = debounce(displayPopupWithData, 250)
            pointMapChart.map().on('movestart', function() {
                // map has started moving in some way, so cancel
                // any debouncing, and hide any current popups.
                mapmove = true
                debouncedPopup.cancel()
                pointMapChart.hidePopup()
            })

            pointMapChart.map().on('moveend', function(event) {
                // map has stopped moving, so start a debounce event.
                // If the cursor is idle, a popup will show if the
                // cursor is over a layer element.
                mapmove = false
                debouncedPopup(event)
                pointMapChart.hidePopup()
            })

            pointMapChart.map().on('mousemove', function(event) {
                // mouse has started moving, so hide any existing
                // popups. 'true' in the following call says to
                // animate the hiding of the popup
                pointMapChart.hidePopup(true)

                // start a debound popup event if the map isn't
                // in motion
                if (!mapmove) {
                    debouncedPopup(event)
                }
            })

            // callback function for when the mouse has been idle for a moment.
            function displayPopupWithData (event) {
                if (event.point) {
                    // check the pointmap for hit-testing. If a layer's element is found under
                    // the cursor, then display a popup of the resulting columns
                    pointMapChart.getClosestResult(event.point, function(closestPointResult) {
                        // 'true' indicates to animate the popup when starting to display
                        pointMapChart.displayPopup(closestPointResult, true)
                    })
                }
            }

            /*--------------------------RESIZE EVENT------------------------------*/
            /* Here we listen to any resizes of the main window.  On resize we resize the corresponding widgets and call dc.renderAll() to refresh everything */
            // window.addEventListener("resize", debounce(reSizeAll, 500))
            //
            // function reSizeAll() {
            //     let w = document.documentElement.clientWidth - 30,
            //         h = Math.max(document.documentElement.clientHeight, window.innerHeight || 0) - 200
            //
            //     pointMapChart
            //         .width(w)
            //         .height(h/1.5)
            //
            //     dc.redrawAllAsync()
            // }
        })
    }
}
