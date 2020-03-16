var app = new Vue({
        el: '#app',
        data: {
            query: '',
            groups: [],
            checkedGroups: [],
        }
    });
$(document).ready(function(){
    const url=""; /** replace with url **/
    $.ajax({
        url: url,
        type: 'GET',
        xhrFields: {
        withCredentials: true
        },
        success: function(result){
            var group_set;
            group_set = JSON.parse(result);
            for (let object in group_set){
                app.groups.push({'name': group_set[object]});
            }
        },
        error: function(error) {
            console.log('Error');
        }
    })
});