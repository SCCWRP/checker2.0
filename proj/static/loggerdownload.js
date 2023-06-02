var projectId;
var validprojectId;
var estuaryName;
var validestuaryName;
var sensorType;
var validsensorType;

var loggerStart;
var loggerEnd;


document.getElementById("open-alert").addEventListener("click", async function () {
    document.getElementById("alert").setAttribute("open","")
})

document.getElementById("logger-start-date").addEventListener("calciteDatePickerChange", async function () {
    document.getElementById("logger-start-label").innerText = `Start Date: ${document.getElementById("logger-start-date").value} 00:00:00`
})

document.getElementById("logger-end-date").addEventListener("calciteDatePickerChange", async function () {
    document.getElementById("logger-end-label").innerText = `End Date: ${document.getElementById("logger-end-date").value} 23:59:59`
})


document.getElementById("confirm").addEventListener("click", async function (){
    document.getElementById('logger-start-date').classList.add("hidden")
    document.getElementById('logger-end-date').classList.add("hidden")
    document.getElementById("confirm").classList.add("hidden")

    loggerStart = document.getElementById("logger-start-date").value
    loggerEnd = document.getElementById("logger-end-date").value

    console.log(loggerStart)
    console.log(loggerEnd)
    document.getElementById("loader-container").classList.remove("hidden")
    const resp = await fetch('/checker/loggerdownload/populate-dropdown', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ 'logger_start': loggerStart, 'logger_end': loggerEnd })
    })
    const res = await resp.json()
        
    validprojectId = res['projectid']
    validestuaryName = res['estuaryname']
    validsensorType = res['sensortype']


    var tmpString = res['projectid'].map(item => { return `<calcite-dropdown-item selected>${item}</calcite-dropdown-item>` });
    document.querySelector('#select-project').getElementsByTagName("calcite-dropdown-group")[0].innerHTML = tmpString.join('');
    document.querySelector('#select-project').getElementsByTagName("calcite-button")[0].innerText = res['projectid'].join(', ')

    var tmpString = res['estuaryname'].map(item => { return `<calcite-dropdown-item selected>${item}</calcite-dropdown-item>` });
    document.querySelector('#select-estuary').getElementsByTagName("calcite-dropdown-group")[0].innerHTML = tmpString.join('');
    document.querySelector('#select-estuary').getElementsByTagName("calcite-button")[0].innerText = res['estuaryname'].join(', ')

    var tmpString = res['sensortype'].map(item => { return `<calcite-dropdown-item selected>${item}</calcite-dropdown-item>` });
    document.querySelector('#select-sensortype').getElementsByTagName("calcite-dropdown-group")[0].innerHTML = tmpString.join('');
    document.querySelector('#select-sensortype').getElementsByTagName("calcite-button")[0].innerText = res['sensortype'].join(', ')

    document.getElementById("loader-container").classList.add("hidden")
    document.getElementById("download-logger-container").classList.remove("hidden")

})

document.getElementById("select-project").addEventListener("calciteDropdownSelect", async function () {

    projectId = Array.from(document.getElementById("select-project").selectedItems).map(item => `'${item.innerHTML}'`).join(",");
    document.querySelector('#select-project').getElementsByTagName("calcite-button")[0].innerText = projectId.split(",").map(item => item.replace(/'/g, "")).join(", ");

    const resp = await fetch('/checker/loggerdownload/repopulate-dropdown', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ 'projectid': projectId })
    })
    const res = await resp.json()

    var tmpString = res.estuaryname.filter(estuary => validestuaryName.includes(estuary)).map(item => { return `<calcite-dropdown-item selected>${item}</calcite-dropdown-item>` });
    document.querySelector('#select-estuary').getElementsByTagName("calcite-dropdown-group")[0].innerHTML = tmpString.join('');
    document.querySelector('#select-estuary').getElementsByTagName("calcite-button")[0].innerText = res.estuaryname.filter(estuary => validestuaryName.includes(estuary)).join(', ')

    var tmpString = res.sensortype.filter(sensortype => validsensorType.includes(sensortype)).map(item => { return `<calcite-dropdown-item selected>${item}</calcite-dropdown-item>` });
    document.querySelector('#select-sensortype').getElementsByTagName("calcite-dropdown-group")[0].innerHTML = tmpString.join('');
    document.querySelector('#select-sensortype').getElementsByTagName("calcite-button")[0].innerText = res.sensortype.filter(sensortype => validsensorType.includes(sensortype)).join(', ')

})

document.getElementById("select-estuary").addEventListener("calciteDropdownSelect", async function () {

    estuaryName = Array.from(document.getElementById("select-estuary").selectedItems).map(item => `'${item.innerHTML}'`).join(",");
    document.querySelector('#select-estuary').getElementsByTagName("calcite-button")[0].innerText = estuaryName.split(",").map(item => item.replace(/'/g, "")).join(", ");

    const resp = await fetch('/checker/loggerdownload/repopulate-dropdown', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ 'projectid': projectId, 'estuaryname': estuaryName })
    })
    const res = await resp.json()

    var tmpString = res.sensortype.filter(sensortype => validsensorType.includes(sensortype)).map(item => { return `<calcite-dropdown-item selected>${item}</calcite-dropdown-item>` });
    document.querySelector('#select-sensortype').getElementsByTagName("calcite-dropdown-group")[0].innerHTML = tmpString.join('');
    document.querySelector('#select-sensortype').getElementsByTagName("calcite-button")[0].innerText = res.sensortype.filter(sensortype => validsensorType.includes(sensortype)).join(', ')

})

document.getElementById("select-sensortype").addEventListener("calciteDropdownSelect", async function () {

    sensorType = Array.from(document.getElementById("select-sensortype").selectedItems).map(item => `'${item.innerHTML}'`).join(",");
    document.querySelector('#select-sensortype').getElementsByTagName("calcite-button")[0].innerText = sensorType.split(",").map(item => item.replace(/'/g, "")).join(", ");

})

document.getElementById("download-logger").addEventListener("click", function () {
    
    projectId = Array.from(document.getElementById("select-project").selectedItems).map(item => `'${item.innerHTML}'`).join(",");
    estuaryName = Array.from(document.getElementById("select-estuary").selectedItems).map(item => `'${item.innerHTML}'`).join(",");
    sensorType = Array.from(document.getElementById("select-sensortype").selectedItems).map(item => `'${item.innerHTML}'`).join(",");

    var valid = true;

    if (!projectId) {
        // Value is falsy (empty, undefined, null, etc.)
        alert('Please provide a project ID');
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

    if (valid) {
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

refreshButton.addEventListener('click', function () {

    location.reload();

});


// document.querySelectorAll('calcite-button#select-all').forEach(function (selectAllButton) {
//     selectAllButton.addEventListener('click', function () {
//         var parentDiv = selectAllButton.parentNode;
//         var dropdownItems = parentDiv.querySelectorAll('calcite-dropdown-item');
//         dropdownItems.forEach(function (dropdownItem) {
//             dropdownItem.selected = true;
//         });
//     });
// });

// document.querySelectorAll('calcite-button#deselect-all').forEach(function (selectAllButton) {
//     selectAllButton.addEventListener('click', function () {
//         var parentDiv = selectAllButton.parentNode;
//         var dropdownItems = parentDiv.querySelectorAll('calcite-dropdown-item');
//         dropdownItems.forEach(function (dropdownItem) {
//             dropdownItem.selected = false;
//         });
//     });
// });
