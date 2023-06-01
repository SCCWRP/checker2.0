var projectId;
var siteId;
var estuaryName;
var sensorType;
var loggerStart;
var loggerEnd;

document.getElementById("select-project").addEventListener("calciteDropdownSelect", async function(){
    
    projectId = Array.from(document.getElementById("select-project").selectedItems).map(item => `'${item.innerHTML}'`).join(",");
    document.querySelector('#select-project').getElementsByTagName("calcite-button")[0].innerText = projectId.split(",").map(item => item.replace(/'/g, "")).join(", ");

    if (projectId != ''){
        const resp = await fetch('/checker/loggerdownload/repopulate-dropdown', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ 'projectid': projectId })
        })
        const res = await resp.json()
        console.log(res)
        
        var tmpString = res['estuaryname'].map(item => `<calcite-dropdown-item>${item}</calcite-dropdown-item>`);
        document.querySelector('#select-estuary').getElementsByTagName("calcite-dropdown-group")[0].innerHTML = tmpString.join('');
        
    } else {
        document.querySelector('#select-project').getElementsByTagName("calcite-button")[0].innerText = "Select a Project";
    }

})

document.getElementById("select-estuary").addEventListener("calciteDropdownSelect", async function(){

    estuaryName = Array.from(document.getElementById("select-estuary").selectedItems).map(item => `'${item.innerHTML}'`).join(",");
    document.querySelector('#select-estuary').getElementsByTagName("calcite-button")[0].innerText = estuaryName.split(",").map(item => item.replace(/'/g, "")).join(", ");

    if (estuaryName != ''){
        const resp = await fetch('/checker/loggerdownload/repopulate-dropdown', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ 'estuaryname': estuaryName })
        })
        const res = await resp.json()
        console.log(res)
        
        tmpString = res['sensortype'].map(item => `<calcite-dropdown-item>${item}</calcite-dropdown-item>`);
        document.querySelector('#select-sensortype').getElementsByTagName("calcite-dropdown-group")[0].innerHTML = tmpString.join('');        
    
    } else {
        document.querySelector('#select-estuary').getElementsByTagName("calcite-button")[0].innerText = "Select an Estuary";
    }

})


document.getElementById("select-sensortype").addEventListener("calciteDropdownSelect", async function(){

    sensorType = Array.from(document.getElementById("select-sensortype").selectedItems).map(item => `'${item.innerHTML}'`).join(",");
    document.querySelector('#select-sensortype').getElementsByTagName("calcite-button")[0].innerText = sensorType.split(",").map(item => item.replace(/'/g, "")).join(", ");

    if (sensorType != ''){
        const resp = await fetch('/checker/loggerdownload/getminmaxtimestamp', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ 'projectid': projectId, 'estuaryname': estuaryName, 'sensortype': sensorType })
        })
        const res = await resp.json()
        console.log(res)
        
    } else {
        document.querySelector('#select-sensortype').getElementsByTagName("calcite-button")[0].innerText = "Select a sensor type";
    }

})


document.getElementById("logger-start-date").addEventListener("calciteDatePickerChange", function(){
    loggerStart = document.getElementById("logger-start-date").value
    document.getElementById("logger-start-label").innerText = `Start Date: ${loggerStart} 00:00:00`
})


document.getElementById("logger-end-date").addEventListener("calciteDatePickerChange", function(){
    loggerEnd = document.getElementById("logger-end-date").value
    document.getElementById("logger-end-label").innerText = `End Date: ${loggerEnd} 23:59:59`
})


document.getElementById("download-logger").addEventListener("click", function(){

    var valid = true;

    if (!projectId) {
        // Value is falsy (empty, undefined, null, etc.)
        alert('Please provide a project ID');
        valid = false;
    }
    
    if (!siteId) {
        // Value is falsy (empty, undefined, null, etc.)
        alert('Please provide a site ID');
        valid = false;
    }
    
    if (!estuaryName) {
        // Value is falsy (empty, undefined, null, etc.)
        alert('Please provide an estuary name');
        valid = false;
    }
    
    if (!sensorType) {
        // Value is falsy (empty, undefined, null, etc.)
        alert('Please provide a sensor type');
        valid = false;
    }
      
    if (loggerStart === undefined) {
        // Value is undefined
        alert('Please pick a start date');
        valid = false;
    } else if (loggerEnd === undefined) {
        // Value is defined
        alert('Please pick an end date');
        valid = false;
    }
    
    // Compare the datetime values
    if (Date.parse(loggerStart) > Date.parse(loggerEnd)) {
        alert('Start Date is greater than End Date');
        valid = false;
    }
    
    if (valid){
        const parentDiv = document.getElementById('download-logger-container');
        const loaderElement = document.createElement('calcite-loader');
        parentDiv.appendChild(loaderElement)
        var jsonObject = {
            'host': 'geodbinstance',
            'database': 'empa2021',
            'base_table': 'tbl_wqlogger',
            'datetime_colname': 'samplecollectiontimestamp',
            'start_time': `${loggerStart} 00:00:00`,
            'end_time': `${loggerEnd} 23:59:59`,
            'projectid': projectId,
            'siteid': siteId,
            'estuaryname': estuaryName,
            'sensortype': sensorType,
            'is_partitioned': 'True'
        };

        // Send the JSON object to the server
        fetch('/checker/getloggerdata', {
            method: 'POST',
            headers: {
            'Content-Type': 'application/json'
            },
            body: JSON.stringify(jsonObject)
        })
        .then(response => response.blob())
        .then(blob => {
            parentDiv.removeChild(loaderElement);
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `logger_${loggerStart}_${loggerEnd}.csv`
            document.body.appendChild(a);
            a.click();
            a.remove();
        })

    } 
})

const refreshButton = document.getElementById('reset-all');

refreshButton.addEventListener('click', function() {

  location.reload();

});