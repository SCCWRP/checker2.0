let datatype = ''
let warningsArray = []
console.log(datatype)

document.querySelector("#datatype-select").addEventListener("change", async (e) => {
   datatype = e.target.value
   let tablesObject = await getTables(datatype)
   let tables = tablesObject.tables
   tableSelect = document.querySelector('#table-select')
   tableSelect.innerHTML = ""
   tables.forEach(table => {
      opt = document.createElement('option')
      opt.innerText = table
      opt.value = table
      tableSelect.appendChild(opt)
   })
})

document.addEventListener("DOMContentLoaded", () => {
   document.getElementsByName('warnings-cbs').forEach(checkbox => {
      console.log(checkbox)
      checkbox.addEventListener('change', (e) => {
         console.log(e.target.value)
      })
   })
})

const getTables = async (dt) => {
   const response = await fetch('/bight23checker/warnings-report', {
      method: "POST",
      headers: {
         "Content-Type" : "application/json"
      },
      body: JSON.stringify({
         'datatype': dt,
         'table': null
      })
   })
   const tables = await response.json()
   return tables
}

function selectWarnings(cb) {
   if (cb.checked === true) {
      warningsArray.push(cb.value)
      console.log(warningsArray)
   }
   if (cb.checked === false) {
      let index = warningsArray.indexOf(cb.value)
      if (index !== -1) {
         warningsArray.splice(index, 1)
      } 
      console.log(warningsArray)
   }
}

function showSpinner() {
   document.getElementById('spinner-container').style.display = 'block';
}

function hideSpinner() {
   document.getElementById('spinner-container').style.display = 'none';
}

async function handleExport(warnings, table) {
   
   if (warningsArray.length === 0) {
      alert('Please select at least one warning you would like to download data for.')
   }
   else {
      showSpinner()
      const response = await fetch('/bight23checker/warnings-report/export', {
         method: "POST",
         headers: {
            "Content-Type" : "application/json"
         },
         body: JSON.stringify({
            'warnings': warningsArray,
            'table': table
         })
      })
      const fileBlob = await response.blob()
      const url = window.URL.createObjectURL(fileBlob)
      const link = document.createElement('a')

      link.href = url
      link.setAttribute('download', `${table}-export.xlsx`)

      document.body.appendChild(link)

      link.click()

      document.body.removeChild(link)

      window.URL.revokeObjectURL(url)
      hideSpinner()
   }
}

function handleSubmitClick() {
   submitBtn = document.querySelector('#submit-btn')
   submitBtn.click()
}