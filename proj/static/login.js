(function (){

    const updateSelectInput = async function (route) {

            console.assert(
                ['pendantid','startdate','enddate'].includes(route),

                {"route":route, "message": "must be 'pendantids','startdates','enddates'"}
            )
            /*  Now go get the collectiondates that correspond to the sitecode they just selected 
            and update the collectiondates input */
            let formData = new FormData();
            formData.append('login_sitecode', document.getElementById('sitecode-select').value);
            formData.append('login_loggernumber', Number(document.querySelector("input[type='radio'][name='login_loggernumber']:checked").value));
            formData.append('login_pendantid', document.getElementById('pendantid-select').value);
            formData.append('login_start', document.getElementById('startdate-select').value)
            
            let response = await fetch(
                // all of the routes on the flask app have an 's' at the end
                // i mean the routes for updating form inputs
                `/${script_root}/${route}s`, 
                {
                    method: 'post',
                    body: formData
                }
            );
            console.log(response);
            const result = await response.json();
            console.log(result);
            let data;
            switch (route) {
                case 'pendantid':
                    data = result.pendantids;
                    break;
                case 'startdate':
                    data = result.startdates;
                    break;
                case 'enddate':
                    data = result.enddates;
                    break;
                default:
                    console.log(`bad arg ${route} passed to updateInputs function`);
                    return;
            }
                
    
            if ( Number(data.length) === 0 ){
                alert(`No ${route} found - please double check the information in the login form`)
                return;
            }
            
            // append data as options to the select item
            data.map(c => {
                switch (route) {
                    case 'enddate':
                        // for enddate, set the innerHTML
                        document.getElementById(`${route}-select`).innerHTML = `
                            <option value="${c}" selected>${c}</option>
                        `
                        // unhide submit button
                        document.getElementById('login-form-submit-btn-container').classList.remove('hidden');
                    default:
                        // for the rest of the cases, append to the HTML
                        document.getElementById(`${route}-select`).innerHTML += `
                            <option value="${c}">${c}</option>
                        `
                }
            });

    }

    /*
    When the user selects a sitecode, pendantids should get displayed
    When the sitecode select input changes, the pendant id input should get hidden from the user
    */
    document.getElementById("sitecode-select").addEventListener('change', async function (){
       
        // reset pendantids and collectiondates when they change the sitecode
        document.getElementById('startdate-select')
            .innerHTML = `<option value="none" selected disabled hidden></option>`;
        document.getElementById('enddate-select')
            .innerHTML = `<option value="none" selected disabled hidden></option>`;
        document.getElementById('pendantid-select')
            .innerHTML = `<option value="none" selected disabled hidden></option>`;

        // since pendantids and collectiondates were reset, we need to hide the submit button again
        document.getElementById('login-form-submit-btn-container').classList.add('hidden');

        updateSelectInput('pendantid')

    })
    
    /*
    When the user selects a pendantid, startdates should get displayed
    When the sitecode select input changes, the below inputs should be reset
    */
    document.getElementById("pendantid-select").addEventListener('change', async function (){
       
        // reset pendantids and collectiondates when they change the sitecode
        document.getElementById('startdate-select')
            .innerHTML = `<option value="none" selected disabled hidden></option>`;
        document.getElementById('enddate-select')
            .innerHTML = `<option value="none" selected disabled hidden></option>`;
        

        // since pendantids and collectiondates were reset, we need to hide the submit button again
        document.getElementById('login-form-submit-btn-container').classList.add('hidden');

        updateSelectInput('startdate')

    })
    
    /*
    When the user selects a startdate, startdates should get displayed
    When the sitecode select input changes, the below inputs should be reset
    */
    document.getElementById("startdate-select").addEventListener('change', async function (){
       
        // reset enddate when they change the sitecode
        document.getElementById('enddate-select')
            .innerHTML = `<option value="none" selected disabled hidden></option>`;
        

        // since startdates and collectiondates were reset, we need to hide the submit button again
        document.getElementById('login-form-submit-btn-container').classList.add('hidden');

        updateSelectInput('enddate')

    })

    // each logger number radio button needs an event listener
    Array.from(document.querySelectorAll("input[type='radio'][name='login_loggernumber']")).map(
        (elem) => {
            elem.addEventListener('change', async function(){
                const lognum = document.querySelector("input[type='radio'][name='login_loggernumber']:checked").value;

                // reset pendantid, and dates when they change the loggernumber
                document.getElementById('pendantid-select')
                    .innerHTML = `<option value="none" selected disabled hidden></option>`;
                document.getElementById('startdate-select')
                    .innerHTML = `<option value="none" selected disabled hidden></option>`;
                document.getElementById('enddate-select')
                    .innerHTML = `<option value="none" selected disabled hidden></option>`;
    
                // since things were reset, we need to hide the submit button again
                document.getElementById('login-form-submit-btn-container').classList.add('hidden');

                updateSelectInput('pendantid')

            })
        }
    )

})()