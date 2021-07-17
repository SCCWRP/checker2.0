
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

    // Let them download their marked excel file
    document.getElementById("excel-markup-download").setAttribute("href",`/${script_root}/download/${res.submissionid}/${res.marked_filename}`) ;
    
    
    // errors
    const errs_tables = [...new Set(res.errs.map(e => e.table))]
    
    // Create the tab headers, as many as there are unique tables in the error report
    document.getElementById("errors-report-tab-headers").innerHTML = document.getElementById("errors-report-tab-headers").innerHTML.repeat(errs_tables.length);
    
    // put the appropriate ID's and inner text on each div ("table") containing the error tab headers
    const errorsHeaders = document.querySelectorAll("#errors-report-tab-headers .errors-tab-header");
    for (let i = 0; i < errorsHeaders.length; i++) {
        errorsHeaders[i].setAttribute('id',`${errs_tables[i]}-errors-tab-header`);
        errorsHeaders[i].innerText = errs_tables[i];
    }
    
    // repeat the error tab bodies as much as there are tables with errors
    document.getElementById("errors-report-body-inner-tab-container").innerHTML = document.getElementById("errors-report-body-inner-tab-container")
        .innerHTML
        .repeat(errs_tables.length);

    // put the appropriate ID's on each div ("table")containing the error messages
    const errorsTabBodies = document.querySelectorAll("#errors-report-body-inner-tab-container .errors-tab-body");
    for (let i = 0; i < errorsTabBodies.length; i++) {
        errorsTabBodies[i].setAttribute('id', `${errs_tables[i]}-errors-tab-body`);
    }
    
    // Now append the rows with the error information
    errs_tables.map(tblname => {
        let tbl = document.querySelector(`#${tblname}-errors-tab-body div.errors-tab-rows`);
        tbl.innerHTML = res.errs.map(e => {
            let s = `
                <div class="error-description-list row">
                    <div class="errors-report-cell error-description-list-item col-sm">
                        ${e.columns}
                    </div>
                    <div class="errors-report-cell error-description-list-item col-sm">
                        ${e.error_type}
                    </div>
                    <div class="errors-report-cell error-description-list-item col-sm">
                        ${e.error_message}
                    </div>
                    <div class="errors-report-cell error-description-list-item col-sm">
                        ${e.rows.map(r => {return `${String(r.row_number)}`}).join(", ") }
                    </div>
                </div>
            `;
            return s
        }).join("")
    })




    // warnings
    document.getElementById("warnings-report-tab-headers").innerHTML = res.errs.map(x => {
        s = `
        <button id="${x.table}-warnings-tab-button" class="warnings-report-tab-button">
            ${x.table}
        </button>
        `;
        return s;
    }).join("") ;

    const warnings_tables = [...new Set(res.warnings.map(w => w.table))]
    document.getElementById("warnings-report-body-inner-tab-container").innerHTML = warnings_tables.map(tblname => {
        let s = `
        <div id="${tblname}-warnings-tab-body" class="warnings-tab-body">
            <table id="${tblname}-warnings-tab-table">
                <thead class="warnings-info-header-list">
                    <tr>
                        <th class="warnings-info-header-list-item">Column(s)</th>
                        <th class="warnings-info-header-list-item">Warning Type</th>
                        <th class="warnings-info-header-list-item">Warning Message</th>
                        <th class="warnings-info-header-list-item">Row(s)</th>
                    </tr>
                </thead>
                <tbody>
                </tbody>
            </table>
        </div>
        `;
        return s
    })
    
    warnings_tables.map(tblname => {
        let tbl = document.querySelector(`#${tblname}-warnings-tab-table tbody`);
        tbl.innerHTML = res.warnings.map(w => {
            // classes still have error since the css will be the same.
            let s = `
                <tr class="error-description-list">
                    <td class="error-description-list-item">
                        ${w.columns}
                    </td>
                    <td class="error-description-list-item">
                        ${w.error_type}
                    </td>
                    <td class="error-description-list-item">
                        ${w.error_message}
                    </td>
                    <td class="error-description-list-item">
                        ${w.rows.map(r => {return `<span class="error-row-number">${String(r.row_number)}, </span>`}).join("") }
                    </td>
                </tr>
            `;
            return s
        }).join("")
    })
    

}

const openTab = (id, classname) => {
    const x = document.getElementsByClassName(classname);
    for (let i = 0; i < x.length; i++) {
      //x[i].style.display = "none";
      x[i].classList.add("hidden");
    }

    document.getElementById(id).style.display = "block";
    //document.getElementById(id).classList.remove("hidden");
}