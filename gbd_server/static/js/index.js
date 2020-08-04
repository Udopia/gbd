var app = new Vue({
    el: '#app',
    data: {
        show_form: true,
        result: [],
        fields: [],
        loading: false,
        form: {
            query: '',
            features: [],
            selected_features: [],
        },
        table: {
            rows: 0,
            sortBy: null,
            sortDesc: false,
            table_busy: false,
            current_page: 1,
            per_page: 10,
            options: [
                {value: 10, text: "10"},
                {value: 20, text: "20"},
                {value: 30, text: "30"},
            ],
            filter: null,
            head_variant: "dark",
        },
        patterns: {
            query_patterns: [
                {value: 'competition_track = main_2019', text: "competition_track = main_2019"},
                {value: 'local like %vliw%', text: "local like %vliw%"},
                {value: 'variables > 5000000', text: "variables > 5000000"},
                {value: '(clauses_horn / clauses) > .9', text: "(clauses_horn / clauses) > .9"},
            ],
        }
    },
    methods: {
        getHost: function () {
            var protocol = location.protocol;
            var slashes = protocol.concat("//");
            var port = location.port;
            return slashes.concat(window.location.hostname).concat(':').concat(port);
        },
        getFeatures: function () {
            $.ajax({
                url: this.getHost().concat("/getfeatures"),
                type: 'GET',
                dataType: 'json',
                success: function (result) {
                    for (let object in result) {
                        app.form.features.push({'text': result[object], 'value': result[object]});
                    }
                },
                error: function (request, status, error) {
                    app.showErrorModal();
                }
            })
        },
        submitQuery: function (event) {
            app.table.filter = ''
            app.table.table_busy = true;
            var jsonData = {
                query: this.form.query,
                selected_features: this.form.selected_features,
            };
            $.ajax({
                url: this.getHost().concat("/results"),
                type: 'POST',
                data: JSON.stringify(jsonData),
                contentType: 'application/json; charset=utf-8',
                dataType: 'json',
                success: function (result) {
                    app.fields = [];
                    app.table.sortBy = null;
                    app.table.sortDesc = false;
                    app.result = result;
                    app.table.rows = result.length
                    var entry = result[0];
                    for (var attribute in entry) {
                        app.fields.push({key: attribute.toString(), sortable: true});
                    }
                    app.table.table_busy = false;
                },
                error: function (request, status, error) {
                    app.table.table_busy = false;
                    app.showErrorModal();
                }
            });
            event.preventDefault();
        },
        showErrorModal() {
            this.$refs['error-modal'].show()
        },
        hideErrorModal() {
            this.$refs['error-modal'].hide()
        },
        onFiltered(filteredItems) {
            // Trigger pagination to update the number of buttons/pages due to filtering
            this.table.rows = filteredItems.length
            this.table.current_page = 1
        },
    },
    mounted: function () {
        this.$nextTick(function () {
            this.getFeatures();
        })
    },
    computed: {
        rows() {
            return this.result.length
        }
    }
});
