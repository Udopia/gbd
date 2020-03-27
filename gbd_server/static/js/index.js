var app = new Vue({
    el: '#app',
    data: {
        show_form: true,
        form: {
            query: '',
            groups: [],
            selected_groups: [],
        },
        result: [],
        table: {
            table_busy: false,
            current_page: 1,
            per_page: 10,
            options: [
                { value: 10 , text: "Show 10 per page"},
                { value: 20 , text: "Show 20 per page"},
                { value: 30 , text: "Show 30 per page"},
            ],
            head_variant: "dark",
        },
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
            app.table.table_busy = true;
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
                    app.table.table_busy = false;
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
            this.show_form = false;
            this.$nextTick(() => {
                this.show_form = true;
            });
        },
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