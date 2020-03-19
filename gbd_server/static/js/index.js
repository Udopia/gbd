var app = new Vue({
        el: '#app',
        data: {
            query: '',
            groups: [],
            selected_groups: [],
        },
        methods: {
            getHost: function() {
                var protocol = location.protocol;
                var slashes = protocol.concat("//");
                var port = location.port;
                return slashes.concat(window.location.hostname).concat(':').concat(port);
            },
            getGroups: function() {
                $.ajax({
                    url: this.getHost().concat("/getgroups"),
                    type: 'GET',
                    dataType: 'json',
                    success: function (result) {
                        for (let object in result) {
                            app.groups.push({'name': result[object]});
                        }
                    },
                    error: function (error) {
                        console.log('Error: '.concat(error.toString()));
                        alert('Something went wrong. Check the console for details.');
                    }
                })
            },
            submitQuery: function(event) {
                var jsonData = {
                        query: this.query,
                        selected_groups: this.selected_groups,
                    };
                $.ajax({
                    url: this.getHost().concat("/results"),
                    type: 'POST',
                    data: JSON.stringify(jsonData),
                    contentType: 'application/json; charset=utf-8',
                    dataType: 'json',
                    success: function (result) {
                        alert('Got result') // TODO: Handle result
                    },
                    error: function (error) {
                        console.log('Error: '.concat(error.toString()));
                        alert('Something went wrong. Check the console for details.');
                    }
                });
            },
        },
        mounted: function () {
                  this.$nextTick(function () {
                    this.getGroups();
                  })
            },
    });