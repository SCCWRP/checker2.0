(function (){

    const updateSelectInput = async function (route) {

            console.assert(
                ['testsite'].includes(route),

                {"route": route, "message": "must be 'testsite'"}
            )
            /*  Now go get the collectiondates that correspond to the sitecode they just selected 
            and update the collectiondates input */
            let formData = new FormData();
            formData.append('login_testsite', document.getElementById('testsite-select').value);
            formData.append('login_dataprovider', document.getElementById('dataprovider-select').value);
            
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


     /*
    When the user selects a agency, startdates should get displayed
    When the sitecode select input changes, the below inputs should be reset
    */
    document.getElementById("dataprovider-select").addEventListener('change', async function (){
       
        // reset pendantids and collectiondates when they change the sitecode
        document.getElementById('testsite-select')
            .innerHTML = `<option value="none" selected disabled hidden></option>`;

        // since testsite was reset, we need to hide the submit button again
        document.getElementById('login-form-submit-btn-container').classList.add('hidden');

        updateSelectInput('testsite')

    })


    /*
    When the user selects a sitecode, pendantids should get displayed
    When the sitecode select input changes, the pendant id input should get hidden from the user
    */
    document.getElementById("testsite-select").addEventListener('change', async function (){
       
        // reset pendantids and collectiondates when they change the sitecode
        document.getElementById('login-form-submit-btn-container').classList.remove('hidden');
        
    })
    
    
    document.getElementById("dataprovider-button").addEventListener('click', async function (){
       
        let dataprovider = prompt("Enter your name or the name of your agency:");
        
        // https://stackoverflow.com/questions/5934113/how-can-i-add-an-option-in-the-beginning
        const select = document.getElementById('dataprovider-select');
        const opt = new Option(text = `${dataprovider}`, value = `${dataprovider}`, defaultSelected = false, selected = true);
        select.insertBefore(opt, select.firstChild);

        // we can get away with this, since the only time after they put in the dataprovider manually is for meta data
        // after they put it manually, it meets the requirements
        if (dataprovider !== '') {
            document.getElementById('login-form-submit-btn-container').classList.remove('hidden');
            document.getElementById('default-hidden-dataprovider-select').remove();

        }
        
    })
    




})()