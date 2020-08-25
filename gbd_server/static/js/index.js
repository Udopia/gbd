var app = new Vue({
    el: '#app',
    data: {
        show_form: true,
        result: [],
        loading: false,
        databases: [],
        form: {
            query: '',
            selected_features: [],
        },
        table: {
            fields: [],
            rows: 0,
            sortBy: null,
            sortDesc: false,
            table_busy: false,
            current_page: 1,
            per_page: 50,
            options: [
                {value: 50, text: "50"},
                {value: 100, text: "100"},
                {value: 150, text: "150"},
            ],
            head_variant: "dark",
        },
        patterns: {
            query_patterns: [
                {value: 'competition_track = main_2020', text: "Main Track 2020"},
                {value: 'competition_track = planning_2020', text: "Planning Track 2020"},
                {value: 'competition_track = main_2019', text: "Main Track 2019"},
                {value: 'filename like %waerden%', text: "Van Der Waerden Numbers"},
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
        getDatabases: function () {
            $.ajax({
                url: this.getHost().concat("/listdatabases"),
                type: 'GET',
                dataType: 'json',
                success: function (result) {
                    for (let object in result) {
                        app.getFeatures(result[object], function(output) {
                           app.databases.push([result[object], output]);
                        });
                    }
                },
                error: function (request, status, error) {
                    app.showErrorModal();
                }
            })
        },
        getFeatures: function (database, handleData) {
            const url = database === undefined ? this.getHost().concat("/listfeatures") : this.getHost().concat("/listfeatures/".concat(database));
            $.ajax({
                url: url,
                type: 'GET',
                dataType: 'json',
                success: function (result) {
                    handleData(result)
                },
                error: function (request, status, error) {
                    app.showErrorModal();
                }
            });
        },
        submitQuery: function (event) {
            app.table.table_busy = true;

            var form = $('#gbdForm');

            $.ajax({
                url: this.getHost().concat("/results"),
                type: 'POST',
                data: form.serialize(),
                dataType: 'json',
                success: function (result) {
                    app.table.fields = [];
                    app.table.sortBy = null;
                    app.table.sortDesc = false;
                    app.result = result;
                    app.table.rows = result.length
                    var entry = result[0];
                    for (var attribute in entry) {
                        app.table.fields.push({key: attribute.toString(), sortable: true});
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
    },
    mounted: function () {
        this.$nextTick(function () {
            this.getDatabases();
            app.form.query = '';
            app.form.selected_features = [];
            this.submitQuery(new Event("click"));
        })
    },
    computed: {
        rows() {
            return this.result.length
        }
    }
});
