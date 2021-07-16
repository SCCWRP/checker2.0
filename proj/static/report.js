
const buildReport = (res) => {

    // submission info
    document.getElementById("submissionid").innerText = res.submissionid;
    document.getElementById("original-filename").innerText = res.filename;
    document.getElementById("submission-type").innerText = res.match_dataset ? res.match_dataset : "Undetermined";
    
    document.getElementById("tab-table-comparison-list").innerHTML = res.match_report.map(
        r => {
            s = r.tablename ? 
            `<li class="list-group-item"><span class="table-matched">MATCH</span> - Sheet ${r.sheetname} matched with table ${r.tablename}</li>` :
            `<li class="list-group-item"><span class="table-not-matched">UNABLE TO MATCH</span> </li>
            <li class="list-group-item"><p>Sheet ${r.sheetname} most closely matched ${r.closest_tbl}</p></li>
            <li class="list-group-item"><p>In table ${r.closest_tbl} but not tab ${r.sheetname}:</p>
            <p>${r.in_table_not_tab}</p></li>
            <li class="list-group-item"><p>In table ${r.sheetname} but not tab ${r.closest_tbl}:</p>
            <p>${r.in_tab_not_table}</p>
            </li>` ;
            return s;
        }
    ).join("");

    // Let them download their marked excel file
    document.getElementById("excel-markup-download").setAttribute("href",`/${script_root}/download/${res.submissionid}/${res.marked_filename}`) ;
    
    
    // errors
    document.getElementById("errors-report-tab-headers").innerHTML = res.errs.map(x => {
        s = `
        <div id="${x.table}-errors-tab-button" class="errors-report-tab-button col-sm" onclick="openTab('${x.table}-errors-tab-body','errors-tab-body')">
            ${x.table}
        </div>
        `;
        return s;
    }).join("") ;
    
    const errs_tables = [...new Set(res.errs.map(e => e.table))]
    document.getElementById("errors-report-body-inner-tab-container").innerHTML = errs_tables.map(tblname => {
        let s = `
        <div id="${tblname}-errors-tab-body" class="errors-tab-body container">
            <div id="${tblname}-errors-tab-table" class="container card">
                
                <div class="row errors-report-header">
                    <div class="col-sm errors-report-cell">Column(s)</div>
                    <div class="col-sm errors-report-cell">Error Type</div>
                    <div class="col-sm errors-report-cell">Error Message</div>
                    <div class="col-sm errors-report-cell">Row(s)</div>
                </div>
                
                <div class="errors-tab-body">
                </div>
            </div>
        </div>
        `;
        return s
    })
    
    errs_tables.map(tblname => {
        let tbl = document.querySelector(`#${tblname}-errors-tab-table div.errors-tab-body`);
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
        <button id="${x.table}-warnings-tab-button" class="warnings-report-tab-button" onclick="openTab('${x.table}-warnings-tab-body','warnings-tab-body')">
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
    let x = document.getElementsByClassName(classname);
    for (let i = 0; i < x.length; i++) {
      //x[i].style.display = "none";
      x[i].classList.add("hidden");
    }
    let th = document.getElementsByClassName("submission-info-tab-header");
    for (let i = 0; i < th.length; i++) {
      //x[i].style.display = "none";
      th[i].classList.remove("active");
    }
   
    //document.getElementById(id).style.display = "block";
    document.getElementById(id).classList.remove("hidden");
    document.getElementById(`${id}-button`).classList.add("active");
}