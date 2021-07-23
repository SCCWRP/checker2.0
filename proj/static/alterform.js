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
        })
    })
})()