
const buildReport = (res) => {

    // submission info
    document.getElementById("submissionid").innerText = res.submissionid;
    document.getElementById("original-filename").innerText = res.filename;
    document.getElementById("submission-type").innerText = res.match_dataset ? res.match_dataset : "Undetermined";
    
    document.getElementById("tab-table-comparison-list").innerHTML = res.match_report.map(
        r => {
            s = r.tablename ? 
            `<li><span class="table-matched">MATCH</span> - Sheet ${r.sheetname} matched with table ${r.tablename}</li>` :
            `<li><span class="table-matched">UNABLE TO MATCH</span> 
            <p>Sheet ${r.sheetname} most closely matched ${r.closest_tbl}</p>
            <p>In table ${r.closest_tbl} but not tab ${r.sheetname}:</p>
            <p>${r.in_table_not_tab}</p>
            <p>In table ${r.sheetname} but not tab ${r.closest_tbl}:</p>
            <p>${r.in_tab_not_table}</p>
            </li>` ;
            return s;
        }
    ).join("");

    document.getElementById("excel-markup-download").setAttribute("href",`/${script_root}/export?submissionid=${res.submissionid}&original_filename=${res.filename}`) ;
    
    
    // errors
    document.getElementById("errors-report-tab-headers").innerHTML = res.errs.map(x => {
        s = `
        <button id="${x.table}-errors-tab-button" class="errors-report-tab-button" onclick="openTab('${x.table}-errors-tab-body','errors-tab-body')">
            ${x.table}
        </button>
        `;
        return s;
    }).join("") ;
    
    document.getElementById("errors-report-body-inner-tab-container").innerHTML = res.errs.map(x => {
        s = `
        <div id="${x.table}-errors-tab-body" class="errors-tab-body">
            
            <ul class="errors-info-header-list">
                <li class="errors-info-header-list-item">Column(s)</li>
                <li class="errors-info-header-list-item">Error Type</li>
                <li class="errors-info-header-list-item">Error Message</li>
                <li class="errors-info-header-list-item">Row(s)</li>
            </ul>

            <ul class="error-description-list">
                <li class="error-description-list-item">
                    ${x.columns}
                </li>
                <li class="error-description-list-item">
                    ${x.error_type}
                </li>
                <li class="error-description-list-item">
                    ${x.error_message}
                </li>
                <li class="error-description-list-item">
                    ${x.rows.map(r => {return String(r.row_number)}).join() }
                </li>
            </ul>

        </div>
        `;
        return s
    })
    
    document.getElementById("warnings-report-body-inner-tab-container").innerHTML = res.errs.map(x => {
        s = `
        <div id="${x.table}-warnings-tab-body" class="warnings-tab-body">
            
            <ul class="warnings-info-header-list">
                <li class="warnings-info-header-list-item">Column(s)</li>
                <li class="warnings-info-header-list-item">Error Type</li>
                <li class="warnings-info-header-list-item">Error Message</li>
                <li class="warnings-info-header-list-item">Row(s)</li>
            </ul>

            <ul class="warning-description-list">
                <li class="warning-description-list-item">
                    ${x.columns}
                </li>
                <li class="warning-description-list-item">
                    ${x.error_type}
                </li>
                <li class="warning-description-list-item">
                    ${x.error_message}
                </li>
                <li class="warning-description-list-item">
                    ${x.rows.map(r => {return String(r.row_number)}).join() }
                </li>
            </ul>

        </div>
        `;
        return s
    })

}

const openTab = (id, classname) => {
    const x = document.getElementsByClassName(classname);
    for (let i = 0; i < x.length; i++) {
      x[i].style.display = "none";
    }
    document.getElementById(id).style.display = "block";
}