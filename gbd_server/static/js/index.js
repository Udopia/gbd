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
        getFeatures: function () {
            $.ajax({
                url: this.getHost().concat("/listfeatures"),
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
            app.table.table_busy = true;

            var form = $('#gbdForm');

            $.ajax({
                url: this.getHost().concat("/results"),
                type: 'POST',
                data: form.serialize(),
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
    },
    mounted: function () {
        this.$nextTick(function () {
            this.getFeatures();
            app.form.query = '';
            app.form.selected_features = [];
            this.submitQuery(new Event( "click" ));
        })
    },
    computed: {
        rows() {
            return this.result.length
        }
    }
});
