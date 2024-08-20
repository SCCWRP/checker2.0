require([
    "esri/config",
    "esri/Map",
    "esri/Graphic",
    "esri/views/MapView",
    "esri/layers/FeatureLayer",
    "esri/widgets/LayerList",
    "esri/widgets/Legend",
    "esri/layers/MapImageLayer",
    "esri/layers/GeoJSONLayer",
    "esri/Graphic",
    "esri/layers/GraphicsLayer"
], function(esriConfig, Map, Graphic, MapView, FeatureLayer, LayerList, Legend, GeoJSONLayer, MapImageLayer, Graphic, GraphicsLayer) {

    function arrayToCssColor(color, opacity = 1) {
        if (typeof(color) === 'string') {
            return color
        }
        return `rgba(${color[0]}, ${color[1]}, ${color[2]}, ${opacity})`;
      }
      
    const blueColors = ["#eff3ff","#bdd7e7","#6baed6","#3182bd","#08519c"];
    const greenColors = ["#edf8e9","#bae4b3","#74c476","#31a354","#006d2c"];
    const purpleColors = ["#f2f0f7","#cbc9e2","#9e9ac8","#756bb1","#54278f"];
    const blueColorsRGBA = [
        "rgba(239, 243, 255, 0.25)",
        "rgba(189, 215, 231, 0.25)",
        "rgba(107, 174, 214, 0.25)",
        "rgba(49, 130, 189, 0.25)",
        "rgba(8, 81, 156, 0.25)"
    ];
        
    const greenColorsRGBA = [
        "rgba(237, 248, 233, 0.25)",
        "rgba(186, 228, 179, 0.25)",
        "rgba(116, 196, 118, 0.25)",
        "rgba(49, 163, 84, 0.25)",
        "rgba(0, 109, 44, 0.25)"
    ];
        
    const purpleColorsRGBA = [
        "rgba(242, 240, 247, 0.25)",
        "rgba(203, 201, 226, 0.25)",
        "rgba(158, 154, 200, 0.25)",
        "rgba(117, 107, 177, 0.25)",
        "rgba(84, 39, 143, 0.25)"
    ];
    const colorForTheSpecifiedRegionOfTheUser = 'rgba(248, 131, 121, 0.45)';  // coral pink
    const targetLatLongColor = [0,255,0];
    const actualGrabLatLongColor = [255,0,0];
    const actualTrawlLineColor = [255,0,0];
    const strataRenderer = {
        type: "unique-value",  // autocasts as new UniqueValueRenderer()
        field: "stratum",
        defaultSymbol: { type: "simple-fill",color: 'rgb(0, 109, 119)' },  
        uniqueValueInfos: [
            {
                // All features with value of "North" will be blue
                value: "Your Region",
                symbol: {
                    type: "simple-fill",  // autocasts as new SimpleFillSymbol()
                    color: colorForTheSpecifiedRegionOfTheUser
                }
            },
            {
                // All features with value of "North" will be blue
                value: "Bay",
                symbol: {
                    type: "simple-fill",  // autocasts as new SimpleFillSymbol()
                    color: greenColorsRGBA[4]
                }
            },
            {
                // All features with value of "North" will be blue
                value: "Marina",
                symbol: {
                    type: "simple-fill",  // autocasts as new SimpleFillSymbol()
                    color: greenColorsRGBA[3]
                }
            },
            {
                // All features with value of "North" will be blue
                value: "Port",
                symbol: {
                    type: "simple-fill",  // autocasts as new SimpleFillSymbol()
                    color: purpleColorsRGBA[2]
                }
            },
            {
                // All features with value of "North" will be blue
                value: "Estuaries",
                symbol: {
                    type: "simple-fill",  // autocasts as new SimpleFillSymbol()
                    color: greenColorsRGBA[1]
                }
            },
            {
                // All features with value of "North" will be blue
                value: "Freshwater Estuary",
                symbol: {
                    type: "simple-fill",  // autocasts as new SimpleFillSymbol()
                    color: greenColorsRGBA[0]
                }
            },
            {
                // All features with value of "North" will be blue
                value: "Inner Shelf",
                symbol: {
                    type: "simple-fill",  // autocasts as new SimpleFillSymbol()
                    color: blueColorsRGBA[0]
                }
            },
            {
                // All features with value of "North" will be blue
                value: "Mid Shelf",
                symbol: {
                    type: "simple-fill",  // autocasts as new SimpleFillSymbol()
                    color: blueColorsRGBA[1]
                }
            },
            {
                // All features with value of "North" will be blue
                value: "Outer Shelf",
                symbol: {
                    type: "simple-fill",  // autocasts as new SimpleFillSymbol()
                    color: blueColorsRGBA[2]
                }
            },
            {
                // All features with value of "North" will be blue
                value: "Upper Slope",
                symbol: {
                    type: "simple-fill",  // autocasts as new SimpleFillSymbol()
                    color: blueColorsRGBA[3]
                }
            },
            {
                // All features with value of "North" will be blue
                value: "Lower Slope",
                symbol: {
                    type: "simple-fill",  // autocasts as new SimpleFillSymbol()
                    color: blueColorsRGBA[4]
                }
            },
            {
                // All features with value of "North" will be blue
                value: "Channel Islands",
                symbol: {
                    type: "simple-fill",  // autocasts as new SimpleFillSymbol()
                    color: purpleColorsRGBA[4]
                }
            }
        ]
    }

    const script_root = sessionStorage.script_root
    
    fetch(`${script_root}/getgeojson`, {
        method: 'POST'
    }).then(
        function (response) 
        {return response.json()
    }).then(function (data) {
        

        const points = data['points']
        const polylines = data['polylines']
        const grab_polygons = data['grab_polygons']
        const trawl_polygons = data['trawl_polygons']
        const strataLayerId = data['strata_layer_id']
        const targets = data['targets']
        
        // New 9/28/2023 - adding bad distances
        const badDistancePoints = data['bad_distance_points']
        const badDistanceLines = data['bad_distance_polylines']


        console.log(points)
        console.log(polylines)
        console.log(grab_polygons)
        console.log(trawl_polygons)

        arcGISAPIKey = data['arcgis_api_key']
        esriConfig.apiKey = arcGISAPIKey
        
        const bightstrata = new FeatureLayer({
            // autocasts as new PortalItem()
            portalItem: {
                id: strataLayerId
            },
            outFields: ["*"],
            renderer: strataRenderer
        });

        const map = new Map({
            basemap: "arcgis-topographic", // Basemap layer service
            layers: [bightstrata]
        });
    
        const view = new MapView({
            map: map,
            center: [-118.193741, 33.770050], //California
            zoom: 10,
            container: "viewDiv",
            extent: { // Set the initial extent of the view to the Southern California Bight region
                xmin: -120.6,
                ymin: 32.5,
                xmax: -117.1,
                ymax: 34.4,
                spatialReference: {
                    wkid: 4326
                }
            }
        });

        
        const graphicsLayer = new GraphicsLayer();
        map.add(graphicsLayer);
        
        let targetStationSymbol = {
            type: "simple-marker",
            color: targetLatLongColor,  // Green
            size: "15px",
            outline: {
                color: [255, 255, 255], // White
                width: 2
            }
        };

        let simpleMarkerSymbol = {
            type: "simple-marker",
            color: actualGrabLatLongColor,  // Red
            size: "15px",
            outline: {
                color: [255, 255, 255], // White
                width: 2
            }
        };

        let simpleLineSymbol = {
            type: "simple-line",
            color: actualTrawlLineColor, // RED
            size: "15px",
            width: '3px'
        };

        let simpleFillSymbol = {
            type: "simple-fill",
            color: colorForTheSpecifiedRegionOfTheUser, 
            size: "15px",
            outline: {
                color: [255, 255, 255],
                width: 1
            }
        };
        

        // let attr = {
        //     Name: "Station out of the specified region", // The name of the pipeline
        //     Recommendation: "Check the Error Tab", // The name of the pipeline
        // };

        
        if (trawl_polygons !== "None" ) {
            let popupTemplate = {
                title: "{region}",
                content: `
                    <p>The Region specified for your trawl: {region}</p>
                    <p>The Stratum: {stratum}</p>
                    <p><strong>The trawl line for your station {stationid} did not intersect this region ({region})</strong></p>
                `
            }
            // let attributes = {
            //     Name: "Bight Strata Layer"
            // }

            console.log('trawl_polygons')
            console.log(trawl_polygons)
            for (let i = 0; i < trawl_polygons.length; i++){
                let polygon = trawl_polygons[i].geometry
                let attributes = trawl_polygons[i].properties
                console.log('polygon')
                console.log(polygon)
                
                let polygonGraphic  = new Graphic({
                    geometry: polygon,
                    symbol: simpleFillSymbol,
                    attributes: attributes,
                    popupTemplate: popupTemplate
                });
                graphicsLayer.add(polygonGraphic);
            }
        } 

        if (grab_polygons !== "None" ) {
            let popupTemplate = {
                title: "Region: {region}, Stratum: {stratum}",
                content: `
                    <p>The Region specified in your sediment grab submission was: {region}</p>
                    <p>The Stratum specified in your sediment grab submission was: {stratum}</p>
                    <p><strong>The Lat/Longs for your sediment grab station {stationid} were not found in this region ({region})</strong></p>
                `
            }
            // let attributes = {
            //     Name: "Bight Strata Layer"
            // }

            console.log('grab_polygons')
            console.log(grab_polygons)
            for (let i = 0; i < grab_polygons.length; i++){
                let polygon = grab_polygons[i].geometry
                let attributes = grab_polygons[i].properties
                console.log('polygon')
                console.log(polygon)
                
                let polygonGraphic  = new Graphic({
                    geometry: polygon,
                    symbol: simpleFillSymbol,
                    attributes: attributes,
                    popupTemplate: popupTemplate
                });
                graphicsLayer.add(polygonGraphic);
            }
        } 
        
        

        
       
        if (targets !== "None" ) {
            let popUp = {
                title: "{stationid} (Target)",
                content: `
                    <p>This point corresponds to the Target LatLongs for the station {stationid}</p>
                    <p>Station {stationid} is in Stratum: {stratum} and Region: {region}</p>
                `
            }
            for (let i = 0; i < targets.length; i++){
                
                let point = targets[i].geometry

                console.log(point)
                
                
                let pointGraphic = new Graphic({
                    geometry: point,
                    symbol: targetStationSymbol,
                    attributes: targets[i].properties,
                    popupTemplate: popUp
                    });

                graphicsLayer.add(pointGraphic);

                // Create a text graphic for the label
                let textGraphic = new Graphic({
                    geometry: point,
                    symbol: {
                        type: "text",
                        color: "black",
                        haloColor: "white",
                        haloSize: "1px",
                        text: targets[i].properties.stationid, // assuming stationid is in properties
                        yoffset: -20,  // Adjust as needed to place the label above the point
                        font: { 
                            size: 12,
                            weight: "bold"
                        }
                    }
                });

                graphicsLayer.add(textGraphic);
            }
        }
        if (points !== "None" ) {
            let popUp = {
                title: "{stationid}",
                content: `
                    <p><strong>Warning: The Lat/Longs given for station {stationid} were not found inside the stratum/region where the station lives (stratum: {stratum}, region: {region})</strong></p>
                    <p>This point corresponds to grab event number: {grabeventnumber}</p>
                `
            }
            for (let i = 0; i < points.length; i++){

                console.log('point')
                console.log(points[i])
                
                
                let actualLatLongPointGraphic = new Graphic({
                    geometry: points[i].geometry,
                    symbol: simpleMarkerSymbol,
                    attributes: points[i].properties,
                    popupTemplate: popUp
                });

                graphicsLayer.add(actualLatLongPointGraphic);
            }
        }
        if (polylines !== "None" ) {
            let popUp = {
                title: "{stationid}",
                content: `
                    <p><strong>Warning: This trawl for {stationid} was not found to intersect the stratum/region where the station lives ({region})</strong></p>
                    <p>This line corresponds to trawl number: {trawlnumber}</p>
                    <p>Start LatLongs: {startlatitude}, {startlongitude}</p>
                    <p>Over LatLongs: {overlatitude}, {overlongitude}</p>
                    <p>End LatLongs: {endlatitude}, {endlongitude}</p>
                `
            }
            for (let i = 0; i < polylines.length; i++){
                let polyline = polylines[i].geometry
                console.log('polyline')
                console.log(polyline)
                
                let polylineGraphic  = new Graphic({
                    geometry: polyline,
                    symbol: simpleLineSymbol,
                    attributes: polylines[i].properties,
                    popupTemplate: popUp
                });
                graphicsLayer.add(polylineGraphic);
            }
        }
        
        if (badDistancePoints !== "None" ) {
            let popUp = {
                title: "{stationid}",
                content: `
                    <p>{error_message}</p>
                    <p>Distance to target measured as: {distance_to_target} meters</p>
                `
            }
            for (let i = 0; i < badDistancePoints.length; i++){

                console.log('point')
                console.log(badDistancePoints[i])
                
                
                let actualLatLongPointGraphic = new Graphic({
                    geometry: badDistancePoints[i].geometry,
                    symbol: simpleMarkerSymbol,
                    attributes: badDistancePoints[i].properties,
                    popupTemplate: popUp
                });

                graphicsLayer.add(actualLatLongPointGraphic);
            }
        }
        
        if (badDistanceLines !== "None" ) {
            let popUp = {
                title: "{stationid}",
                content: `
                    <p>{error_message}</p>
                    <p>Distance to target measured as: {distance_to_target} meters</p>
                `
            }
            for (let i = 0; i < badDistanceLines.length; i++){
                
                let polylineGraphic  = new Graphic({
                    geometry: badDistanceLines[i].geometry,
                    symbol: simpleLineSymbol,
                    attributes: badDistanceLines[i].properties,
                    popupTemplate: popUp
                });
                graphicsLayer.add(polylineGraphic);
            }
        }
        
        const customLegendDiv = document.createElement('div');
        customLegendDiv.setAttribute('id','custom-legend');
        // style="position: absolute; bottom: 10px; left: 10px; background-color: white; padding: 10px;"
        customLegendDiv.style.position = 'absolute';
        customLegendDiv.style.bottom = '25px';
        customLegendDiv.style.left = '10px';
        customLegendDiv.style.backgroundColor = 'white';
        customLegendDiv.style.padding = '10px';

        document.getElementById('viewDiv').appendChild(customLegendDiv)

        const legendData = [
            {
                symbol: targetStationSymbol,
                label: "Target LatLongs"
            },
            {
                symbol: simpleMarkerSymbol,
                label: "Grab"
            },
            {
                symbol: simpleLineSymbol,
                label: "Trawl"
            },
            {
                symbol: simpleFillSymbol,
                label: "Region of Target Station"
            }
        ]
        function createCustomLegend(legendData) {
            const legendContainer = document.getElementById("custom-legend");
          
            legendData.forEach((item) => {
              // Create a symbol element
              const symbolElement = document.createElement("div");
              symbolElement.style.display = "inline-block";
              symbolElement.style.marginRight = "10px";
              symbolElement.style.width = "24px";
              symbolElement.style.height = "24px";
          
              if (item.symbol.type === "simple-marker") {
                symbolElement.style.backgroundColor = arrayToCssColor(item.symbol.color);
                symbolElement.style.border = `${item.symbol.outline.width}px solid ${arrayToCssColor(item.symbol.outline.color)}`;
              } else if (item.symbol.type === "simple-line") {
                symbolElement.style.borderTop = `2px solid ${arrayToCssColor(item.symbol.color)}`;
                symbolElement.style.transform = `rotate(45deg)`;
                symbolElement.style.transformOrigin = `left`;
              } else if (item.symbol.type === "simple-fill") {
                symbolElement.style.backgroundColor = arrayToCssColor(item.symbol.color, 0.6); // Set opacity to 0.6
                symbolElement.style.border = `${item.symbol.outline.width}px solid ${arrayToCssColor(item.symbol.outline.color)}`;
              }
          
              // Create a label element
              const labelElement = document.createElement("span");
              labelElement.textContent = item.label;
          
              // Create a container for the symbol and label
              const containerElement = document.createElement("div");
              containerElement.style.display = "flex";
              containerElement.style.alignItems = "center";
              containerElement.style.marginBottom = "5px";
              
              if (item.symbol.type === 'simple-marker') {
                  // make the border radius a large number so the element becomes circular
                  symbolElement.style.borderRadius = "5000px"; 
              }
          
              // Add the symbol and label to the container
              containerElement.appendChild(symbolElement);
              containerElement.appendChild(labelElement);
          
              // Add the container to the legend
              legendContainer.appendChild(containerElement);
            });
          }
          

        createCustomLegend(legendData);
        // bightstrata.load().then(() => {

        //     const legend = new Legend({
        //         view: view,
        //         container: document.createElement('div'),
        //         layerInfos: [
        //             {
        //                 layer: bightstrata,
        //                 title: 'Bight Strata',
        //             },
        //         ],
        //     });
            
        //     //document.getElementById("viewDiv").appendChild(legend.container);
        //     view.ui.add(legend, "bottom-left");
        // })
        
    
        // const legendExpand = new Expand({
        //     view: view,
        //     content: legend.container,
        //     group: "bottom-left",
        //     expanded: true,
        //   });
          

        
        
    })
      
});