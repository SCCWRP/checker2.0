(function (){

    const updateSelectInput = async function (route) {

            // console.assert(
            //     ['testsite'].includes(route),

            //     {"route": route, "message": "must be 'testsite'"}
            // )

            /*  Now go get the collectiondates that correspond to the sitecode they just selected 
            and update the collectiondates input */
            let formData = new FormData();
            formData.append('login_estuary', document.getElementById('estuary-select').value);
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

            // in cases where there are many login fields, switch case makes more sense
            switch (route) {
                case 'startdate':
                    data = result.startdates;
                    break;
                default:    
                    console.log(`bad arg ${route} passed to updateInputs function`);
                    return;
            }
            
            console.log("data");
            console.log(data);
    
            if ( Number(data.length) === 0 ){
                // yes, for the BMP project, this will always say Testsite unless they change something
                // always better to keep it flexible in case they request more features
                alert(`No ${route} found for this agency`)
                return;
            }
            
            // append data as options to the select item
            data.map(c => {
                switch (route) {
                    case 'startdates':
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
    document.getElementById("estuary-select").addEventListener('change', async function (){
       
        // reset startdate when they change the estuary
        document.getElementById('startdate-select')
            .innerHTML = `<option value="none" selected disabled hidden></option>`;
        // document.getElementById('login-form-submit-btn-container').classList.remove('hidden');
        // if (document.querySelector("input[type='radio'][name='login_datatype']:checked").value === 'calibration') {
        //     // if they are submitting calibration, the submit button should not be hidden
        //     document.getElementById('login-form-submit-btn-container').classList.remove('hidden');
        // } else {
        //     // since startdate were reset, we need to hide the submit button again
        //     // also this should only happen if they are not submitting calibration
        //     document.getElementById('login-form-submit-btn-container').classList.add('hidden');
        // }

        updateSelectInput('startdate')

    })





    document.getElementById('startdate-select').addEventListener('change',function(){
        document.getElementById('login-form-submit-btn-container').classList.remove('hidden');
    })



})()