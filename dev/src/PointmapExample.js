import { debounce } from 'lodash'
// todo - finish cleaning this up to make a good standalone example w/o jQuery, weird 'this', etc.
// todo - [andre]: I question the direct use of the google map api vs abstracting it away
/*----------------BACKEND RENDERED POINT MAP EXAMPLE-----------------------*/
export default function PointmapExample(dc, crossFilter, colorScheme, dim) {

    const  langOriginColors = ["#27aeef", "#ea5545", "#87bc45", "#b33dc6", "#f46a9b", "#ede15b", "#bdcf32", "#ef9b20", "#4db6ac", "#edbf33", "#7c4dff"],
        pointMapDim = crossFilter.dimension(null).projectOn(["conv_4326_900913_x(lon) as x", "conv_4326_900913_y(lat) as y", "lang as color", "followers as size"]),
        xDim = crossFilter.dimension("lon"),
        yDim = crossFilter.dimension("lat"),
        parent = document.getElementById("pointmap-example-container")
    let langDomain = ['en', 'pt', 'es', 'in', 'und', 'ja', 'tr', 'fr', 'tl', 'ru', 'ar', 'th', 'it', 'nl', 'sv', 'ht', 'de', 'et', 'pl', 'sl', 'ko', 'fi', 'lv', 'sk', 'uk', 'da', 'zh', 'ro', 'no', 'cy', 'iw', 'hu', 'bg', 'lt', 'bs', 'vi', 'el', 'is', 'hi', 'hr', 'fa', 'ur', 'ne', 'ta',  'sr', 'bn', 'si', 'ml', 'hy', 'lo', 'iu', 'ka', 'ps', 'te', 'pa', 'am', 'kn', 'chr', 'my', 'gu', 'ckb', 'km', 'ug', 'sd', 'bo', 'dv'],

        langColors = []

    mapLangColors(40)

    const sizeScale = d3.scale.linear().domain([0,5000]).range([2,12])

    const pointMapChart = dc.rasterChart(parent, true)
        .con(con)
        .height(h/1.5)
        .width(w)
        .mapUpdateInterval(750)
        // .mapStyle('json/dark-v8.json') // mapbox default

    const pointLayer = dc.rasterLayer("pointmap-example")
        .dimension(pointMapDim)
        .group(pointMapDim)
        .cap(500000)
        .sampling(true)
        .sizeAttr("size")
        .dynamicSize(d3.scale.sqrt().domain([20000,0]).range([1.0,7.0]).clamp(true))
        .sizeScale(sizeScale)
        .xAttr("x")
        .yAttr("y")
        .xDim(xDim)
        .yDim(yDim)
        .fillColorAttr("color")
        .defaultFillColor("#80DEEA")
        .fillColorScale(d3.scale.ordinal().domain(langDomain).range(langColors))
        .popupColumns(['tweet_text', 'sender_name', 'tweet_time', 'lang', 'origin', 'followers'])

    function mapLangColors(n) {
        langDomain = langDomain.slice(0, n)
        for (let i = 0; i < langDomain.length; i++) {
            langColors.push(langOriginColors[i%langOriginColors.length])
        }
    }

    pointMapChart.pushLayer("points", pointLayer).init()
        .then((chart) => {
        // custom click handler with just event data (no network calls)
        pointMapChart.map().on('mouseup', logClick)
        function logClick (result) {
            console.log("clicked!", result)
        }
        // disable with pointMapChart.map().off('mouseup', logClick)
        // custom click handler with event and nearest row data
        pointMapChart.map().on('mouseup', logClickWithData)

        function logClickWithData (event) {
            pointMapChart.getClosestResult(event.point, function(result){
                console.log(result && result.row_set[0])
            })
        }

        // hover effect with popup
        const debouncedPopup = debounce(displayPopupWithData, 250)
        pointMapChart.map().on('mousewheel', pointMapChart.hidePopup);
        pointMapChart.map().on('mousemove', pointMapChart.hidePopup)
        pointMapChart.map().on('mousemove', debouncedPopup)

        function displayPopupWithData (event) {
            pointMapChart.getClosestResult(event.point, pointMapChart.displayPopup)
        }

        initGeocoder(pointMapChart)
        /* Find additional mapbox styles here:
         *
         * https://github.com/mapbox/mapbox-gl-styles/tree/master/styles
         */
        /*
         * Callback used for JSONP request for Google's Geocoder
         */
        let globalGeocoder = null,
            geocoder       = null,
            geocode        = null,
            geocoderInput  = null,
            geocoderObject = null

        function mapApiLoaded() {
            globalGeocoder = new google.maps.Geocoder()
            geocoderObject.geocoder = globalGeocoder
        }
        /*
         * Style and position your geocoder textbox here using standard jQuery
         * Also, set any event listeners you'd like.
         */
        function initGeocoder (chart) {
            geocoder = new Geocoder(chart)
            geocoder.init(chart.map())
            geocoderInput = $('<input class="geocoder-input" type="text" placeholder="Zoom to"></input>').appendTo($("#chart1-example"))
            geocoderInput.css({ top: '5px', right: '5px', float: 'right', position: 'absolute'})

            // set a key-up event handler for the enter key
            geocoderInput.keyup(function(e) {
                if(e.keyCode === 13) { this.geocoder.geocode(geocoderInput.val()) }
            })
        }
        /*
         * The Geocoder wrapper for Google Maps' geocoder.
         * Has an ultra-small API that simply allows for geocoding a placeName.
         */
        function Geocoder(chart) {
            geocoder = null

            geocode = function(placeName) {
                geocoder.geocode({ 'address': placeName }, _onResult)
            }

            const _onResult = (data, status) => {
                if (status != google.maps.GeocoderStatus.OK) {
                    //throw "Geocoder error";
                    return null
                }
                const viewport = data[0].geometry.viewport
                let sw = viewport.getSouthWest(),
                 ne = viewport.getNorthEast()

                chart.map().fitBounds([ // api specifies lng/lat pairs
                    [sw.lng(), sw.lat()],
                    [ne.lng(), ne.lat()]
                ], { animate: false }) // set animate to true if you want to pan-and-zoom to the location
            }

            init = function() {
                if (globalGeocoder === null) { // have to give global callback for when the loaded google js is executed
                    geocoderObject = this
                    $.getScript("https://maps.google.com/maps/api/js?sensor=false&async=2&callback=mapApiLoaded", function() {})
                }
                else {
                    geocoder = globalGeocoder
                }
            }
        }
})
}
