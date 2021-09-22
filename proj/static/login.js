(function (){

    const updateSelectInput = async function (route) {

            // console.assert(
            //     ['testsite'].includes(route),

            //     {"route": route, "message": "must be 'testsite'"}
            // )

            /*  Now go get the collectiondates that correspond to the sitecode they just selected 
            and update the collectiondates input */
            let formData = new FormData();
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
                case 'testsite':
                    data = result.testsites;
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
            switch (route) {
                case 'testsite':
                    data.map(c => {
                        // for enddate, set the innerHTML
                        document.getElementById(`${route}-select`).innerHTML += `
                            <option value="${c.siteid}">${c.sitename}</option>
                        `
                    })
                    // unhide submit button
                    document.getElementById('login-form-submit-btn-container').classList.remove('hidden');
                default:
                    // Typically we wont return responses as key value pairs
                    
            }
            

    }

    document.getElementById('agency-select').addEventListener('change',function(){
        document.getElementById('login-form-submit-btn-container').classList.remove('hidden');
    })



})()