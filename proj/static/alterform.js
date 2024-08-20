// Changes the form inputs based on the selected value of the datatype radio button
// (function(){
//     Array.from(document.getElementsByName("login_datatype")).map((elem)=>{
//         elem.addEventListener('change', function(){
//             Array.from(document.querySelectorAll('.form-group-dynamic')).map((inp) => {
//                 inp.classList.add('hidden');
//             });
//             Array.from(document.querySelectorAll(`.form-group-dynamic.${this.value}-required`)).map((inp) => {
//                 inp.classList.remove('hidden');
//             });

//             const dtype = document.querySelector("#datatype-select").value;
            
//             switch (dtype) {
//                 case 'monitoring':
//                     if (
//                         document.querySelector('#dataprovider-select').value !== "none" & 
//                         document.querySelector('#login_email').value !== "" &
//                         document.querySelector('#testsite-select').value !== "none" 
//                     ) {
//                         document.getElementById('login-form-submit-btn-container').classList.remove('hidden');
//                     }
//                 default:
//                     if (
//                         document.querySelector('#dataprovider-select').value !== "none" & 
//                         document.querySelector('#login_email').value !== ""
//                     ) {
//                         document.getElementById('login-form-submit-btn-container').classList.remove('hidden');
//                     }
//             }

//         })
//     })
// })()