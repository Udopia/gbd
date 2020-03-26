var app = new Vue({
    el: '#app',
    data: {
        form: {
            query: '',
            groups: [],
            selected_groups: [],
        },
        show_form: true,
        result: [],
        table_busy: false,
        current_page: 1,
        per_page: 5,
    },
    methods: {
        getHost: function () {
            var protocol = location.protocol;
            var slashes = protocol.concat("//");
            var port = location.port;
            return slashes.concat(window.location.hostname).concat(':').concat(port);
        },
        getGroups: function () {
            $.ajax({
                url: this.getHost().concat("/getgroups"),
                type: 'GET',
                dataType: 'json',
                success: function (result) {
                    for (let object in result) {
                        app.form.groups.push({'text': result[object], 'value': result[object]});
                    }
                },
                error: function (error) {
                    console.log('Error: '.concat(error.toString()));
                    alert('Something went wrong. Check the console for details.');
                }
            })
        },
        submitQuery: function (event) {
            app.table_busy = true;
            var jsonData = {
                query: this.form.query,
                selected_groups: this.form.selected_groups,
            };
            $.ajax({
                url: this.getHost().concat("/results"),
                type: 'POST',
                data: JSON.stringify(jsonData),
                contentType: 'application/json; charset=utf-8',
                dataType: 'json',
                success: function (result) {
                    app.result = result;
                    app.table_busy = false;
                },
                error: function (error) {
                    console.log('Error: '.concat(error.toString()));
                    alert('Something went wrong. Check the console for details.');
                }
            });
            event.preventDefault();
        },
        onReset: function (event) {
            event.preventDefault();
            this.form.query = '';
            this.form.selected_groups = [];
            this.show = false;
            this.$nextTick(() => {
                this.show = true;
            });
        }
    },
    mounted: function () {
        this.$nextTick(function () {
            this.getGroups();
        })
    },
    computed: {
        rows() {
            return this.result.length
        }
    }
});