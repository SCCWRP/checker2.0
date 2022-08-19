
const buildReport = (res) => {

    // submission info
    document.getElementById("submissionid").innerText = res.submissionid;
    document.getElementById("original-filename").innerText = res.filename;
    document.getElementById("submission-type").innerHTML = res.match_dataset ? res.match_dataset : "Undetermined (<strong>Unable to match submission with any data type</strong>)";
    

    document.getElementById("tab-table-comparison-list").innerHTML = res.match_report.map(
        r => {
            s = r.tablename ? 
            `<li class="list-group-item"><span class="table-matched">MATCH</span> - Sheet ${r.sheetname} matched with table ${r.tablename}</li>` :
            `<li class="list-group-item">
                <div class="card">
                    <div class="card-header">
                        <span class="table-not-matched">UNABLE TO MATCH</span> Excel sheet ${r.sheetname} with any table (<strong><u>most closely matched ${r.closest_tbl}</u></strong>)
                    </div> 
                    <div class="card-body">
                        <!--<ul>-->
                            <!--<li class="list-group-item">-->
                                <p>In database table ${r.closest_tbl} but <em><u>not</u></em> Excel tab ${r.sheetname}:</p>
                                <p>${r.in_table_not_tab}</p>
                                <hr>
                            <!--</li>-->
                            <!--<li class="list-group-item">-->
                                <p>In Excel tab ${r.sheetname} but <em><u>not</u></em> database table ${r.closest_tbl}:</p>
                                <p>${r.in_tab_not_table}</p>
                            <!--</li> -->
                        <!--</ul>-->
                    </div>
                </div>
            </li>
            `;
            return s;
        }
    ).join("");

    // stop if no dataset was matched
    if(!res.match_dataset) return;

    // Let them download their marked excel file
    document.getElementById("excel-markup-download").classList.remove('hidden')
    document.getElementById("excel-markup-download").setAttribute("href",`/${script_root}/download/${res.submissionid}/${res.marked_filename}`) ;
    
    // Error report summary table for the front page
    if (res.errs) {
        const totalErrorsCount = res.errs.length;
        const coreErrorsCount = res.errs.filter(err => {return err.is_core_error}).length;
        const customErrorsCount = res.errs.filter(err => {return !err.is_core_error}).length;
        const warningsCount = res.warnings ? res.warnings.length : 0;
        document.getElementById('total-errors-count').innerText = totalErrorsCount;
        document.getElementById('core-errors-count').innerText = coreErrorsCount;
        document.getElementById('custom-errors-count').innerText = customErrorsCount;
        document.getElementById('total-warnings-count').innerText = warningsCount;
    }

    // errors
    const errs_tables_headers = [...new Set(res.errs.map(e => res.table_to_tab_map[e.table]))]
    const errs_tables = [...new Set(res.errs.map(e => e.table))]
    
    // Create the tab headers, as many as there are unique tables in the error report
    document.getElementById("errors-report-tab-headers").innerHTML = `<div class="errors-tab-header tab-header col-sm"></div>`.repeat(errs_tables.length);
    
    // put the appropriate ID's and inner text on each div ("table") containing the error tab headers
    let errorsHeaders = document.querySelectorAll("#errors-report-tab-headers .errors-tab-header");
    for (let i = 0; i < errorsHeaders.length; i++) {
        errorsHeaders[i].setAttribute('id',`${errs_tables[i]}-errors-tab-header`);
        errorsHeaders[i].innerText = errs_tables_headers[i];
    }
    
    // repeat the error tab bodies as much as there are tables with errors
    // document.getElementById("errors-report-body-inner-tab-container").innerHTML = document.getElementById("errors-report-body-inner-tab-container")
    //     .innerHTML
    //     .repeat(errs_tables.length > 0 ? errs_tables.length : 1) ;
    document.getElementById("errors-report-body-inner-tab-container").innerHTML = `
        <!--This div needs to be repeated, one per table-->
        <div class="errors-tab-body">
        <!--id would be tablename-errors-tab-body-->

            <div class="row errors-report-header">
                <div class="col-sm errors-report-cell">Column(s)</div>
                <!--<div class="col-sm errors-report-cell">Error Type</div>-->
                <div class="col-sm errors-report-cell">Error Message</div>
                <div class="col-sm errors-report-cell">Row(s)</div>
            </div>

            <div class="errors-tab-rows">
            </div>

        </div>`
        .repeat(errs_tables.length) ;



    // put the appropriate ID's on each div ("table")containing the error messages
    let errorsTabBodies = document.querySelectorAll("#errors-report-body-inner-tab-container .errors-tab-body");
    for (let i = 0; i < errorsTabBodies.length; i++) {
        errorsTabBodies[i].setAttribute('id', `${errs_tables[i]}-errors-tab-body`);
    }
    
    // Now append the rows with the error information
    errs_tables.map(tblname => {
        let tbl = document.querySelector(`#${tblname}-errors-tab-body div.errors-tab-rows`);
        
        tbl.innerHTML = res.errs.map(e => {
            if (e.table === tblname) {
                let s = `
                    <div class="error-description-list row">
                        <div class="errors-report-cell error-description-list-item col-sm">
                            ${e.columns.replaceAll(",","<br>")}
                        </div>
                        <!--<div class="errors-report-cell error-description-list-item col-sm">
                            ${e.error_type}
                        </div>-->
                        <div class="errors-report-cell error-description-list-item col-sm">
                            ${e.error_message}
                        </div>
                        <div class="errors-report-cell error-description-list-item col-sm">
                            ${e.rows.join(", ") }
                        </div>
                    </div>
                `;
                return s
            } else {
                return ''
            }
        }).join("")
    })

    // set up the tabbing system
    let tabIDs = new Object();
    errorsHeaders = document.querySelectorAll("#errors-report-tab-headers .errors-tab-header");
    errorsTabBodies = document.querySelectorAll("#errors-report-body-inner-tab-container .errors-tab-body");
    for (let i = 0; i < errorsHeaders.length; i++) {
        tabIDs[errorsHeaders[i].getAttribute('id')] = errorsTabBodies[i].getAttribute('id')
    }
    tabs(
        tabClass = 'errors-tab', 
        tabIDs = tabIDs
    )



    /* 
    I am fully aware that this is a disgusting violation of the DRY (Don't Repeat Yourself) Principle
    But its been 3 weeks of getting this thing done, we are almost there, it needs to get done ASAP, there's only one place where that above errors display routine
    will be repeated, so i think its ok.
    
    If we have time in the future we can think of a way to generalize it, but there's not so much of a need to do so in this particular case
    */


    // warnings
    const warnings_tables_headers = [...new Set(res.warnings.map(e => res.table_to_tab_map[e.table]))]
    const warnings_tables = [...new Set(res.warnings.map(e => e.table))]

    // Create the tab headers, as many as there are unique tables in the error report
    //document.getElementById("warnings-report-tab-headers").innerHTML = document.getElementById("warnings-report-tab-headers").innerHTML.repeat(warnings_tables.length);
    document.getElementById("warnings-report-tab-headers").innerHTML = `<div class="warnings-tab-header tab-header col-sm"></div>`.repeat(warnings_tables.length);

    // put the appropriate ID's and inner text on each div ("table") containing the error tab headers
    let warningsHeaders = document.querySelectorAll("#warnings-report-tab-headers .warnings-tab-header");
    for (let i = 0; i < warningsHeaders.length; i++) {
        warningsHeaders[i].setAttribute('id',`${warnings_tables[i]}-warnings-tab-header`);
        warningsHeaders[i].innerText = warnings_tables_headers[i];
    }

    // repeat the error tab bodies as much as there are tables with warnings
    document.getElementById("warnings-report-body-inner-tab-container").innerHTML = `
        <!--This div needs to be repeated, one per table-->
        <div class="warnings-tab-body">
            <!--id would be tablename-warnings-tab-body-->

            <div class="row warnings-report-header">
                <div class="col-sm warnings-report-cell">Column(s)</div>
                <!--<div class="col-sm warnings-report-cell">Error Type</div>-->
                <div class="col-sm warnings-report-cell">Error Message</div>
                <div class="col-sm warnings-report-cell">Row(s)</div>
            </div>

            <div class="warnings-tab-rows">
            </div>

        </div>`
        .repeat(warnings_tables.length);


    // put the appropriate ID's on each div ("table")containing the error messages
    let warningsTabBodies = document.querySelectorAll("#warnings-report-body-inner-tab-container .warnings-tab-body");
    for (let i = 0; i < warningsTabBodies.length; i++) {
        warningsTabBodies[i].setAttribute('id', `${warnings_tables[i]}-warnings-tab-body`);
    }

    // Now append the rows with the error information
    warnings_tables.map(tblname => {
        console.log(tblname);
        let tbl = document.querySelector(`#${tblname}-warnings-tab-body div.warnings-tab-rows`);
        tbl.innerHTML = res.warnings.map(e => {
            if (e.table === tblname) {
                let s = `
                    <div class="error-description-list row">
                        <div class="warnings-report-cell error-description-list-item col-sm">
                            ${e.columns.replaceAll(",","<br>")}
                        </div>
                        <!--<div class="warnings-report-cell error-description-list-item col-sm">
                            ${e.error_type}
                        </div>-->
                        <div class="warnings-report-cell error-description-list-item col-sm">
                            ${e.error_message}
                        </div>
                        <div class="warnings-report-cell error-description-list-item col-sm">
                            ${e.rows.join(", ") }
                        </div>
                    </div>
                `;
                return s
            } else {
                return ''
            }
        }).join("")
    })

    // set up the tabbing system
    tabIDs = new Object();
    warningsHeaders = document.querySelectorAll("#warnings-report-tab-headers .warnings-tab-header");
    warningsTabBodies = document.querySelectorAll("#warnings-report-body-inner-tab-container .warnings-tab-body");
    for (let i = 0; i < warningsHeaders.length; i++) {
        tabIDs[warningsHeaders[i].getAttribute('id')] = warningsTabBodies[i].getAttribute('id')
    }
    tabs(
        tabClass = 'warnings-tab', 
        tabIDs = tabIDs
    )


    // display the map if applicable
    document.getElementById('visual-map').setAttribute('src',`/${script_root}/map/${res.submissionid}/${res.match_dataset}`)



    
    

}

