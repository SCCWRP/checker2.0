// Changes the form inputs based on the selected value of the datatype radio button
(function(){
    Array.from(document.getElementsByName("login_datatype")).map((elem)=>{
        elem.addEventListener('change', function(){
            Array.from(document.querySelectorAll('.form-group-dynamic')).map((inp) => {
                inp.classList.add('hidden');
            });
            Array.from(document.querySelectorAll(`.form-group-dynamic.${this.value}-required`)).map((inp) => {
                inp.classList.remove('hidden');
            });

            const dtype = document.querySelector("input[type='radio'][name='login_datatype']:checked").value;
            
            if (dtype === 'calibration') {
                if (document.querySelector('#sitecode-select').value !== "none") {
                    document.getElementById('login-form-submit-btn-container').classList.remove('hidden');
                }
            }
        })
    })
})()