var loggerStart;
var loggerEnd;

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
        alert("Your download will start shortly")
        var jsonObject = {
            'host': 'geodbinstance',
            'database': 'empa2021',
            'base_table': 'tbl_wqlogger',
            'datetime_colname': 'samplecollectiontimestamp',
            'start_time': `${loggerStart} 00:00:00`,
            'end_time': `${loggerEnd} 23:59:59`,
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