(function (){
    /*
    When the user selects a sitecode, collectiondates should get displayed
    When the sitecode select input changes, the pendant id input should get hidden from the user
    */
    document.getElementById("sitecode-select").addEventListener('change', async function (){
       
        // reset pendantids and collectiondates when they change the sitecode
        document.getElementById('collectiondate-select')
            .innerHTML = `<option value="none" selected disabled hidden></option>`;
        document.getElementById('pendantid-select')
            .innerHTML = `<option value="none" selected disabled hidden></option>`;

        // since pendantids and collectiondates were reset, we need to hide the submit button again
        document.getElementById('login-form-submit-btn-container').classList.add('hidden');

        /*  Now go get the collectiondates that correspond to the sitecode they just selected 
            and update the collectiondates input */
        let formData = new FormData();
        formData.append('login_sitecode', document.getElementById('sitecode-select').value)
        let response = await fetch(
            `/${script_root}/collectiondates`, 
            {
                method: 'post',
                body: formData
            }
        );
        console.log(response);
        const result = await response.json();
        console.log(result);

        collectiondates = result.collectiondates;
        
        // append collectiondates as options to the collectiondate select item
        collectiondates.map(c => {
            document.getElementById('collectiondate-select').innerHTML += `
                <option value="${c}">${c}</option>
            `
        });

    })


    // same principle applies to the collection dates, go get the corresponding pendant ids
    document.getElementById("collectiondate-select").addEventListener('change', async function (){

        // reset pendantids when they change the collectiondate
        document.getElementById('pendantid-select')
            .innerHTML = `<option value="none" selected disabled hidden></option>`;
        
        // since pendantids were reset, we need to hide the submit button again
        document.getElementById('login-form-submit-btn-container').classList.add('hidden');

        /*  Now go get the collectiondates that correspond to the collectiondate they just selected 
        and update the collectiondates input */
        let formData = new FormData();
        formData.append('login_sitecode', document.getElementById('sitecode-select').value);
        formData.append('login_collectiondate', document.getElementById('collectiondate-select').value);
        let response = await fetch(
            `/${script_root}/pendantids`, 
            {
                method: 'post',
                body: formData
            }
        );
        console.log(response);
        const result = await response.json();
        console.log(result);

        pendantids = result.pendantids;
        
        // append pendantids as options to the pendantid select item
        pendantids.map(c => {
            document.getElementById('pendantid-select').innerHTML += `
                <option value="${c}">${c}</option>
            `
        });
    })

    document.getElementById('pendantid-select').addEventListener('change', () => {
        document.getElementById('login-form-submit-btn-container').classList.remove('hidden');
    })
})()