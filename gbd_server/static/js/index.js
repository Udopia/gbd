var app = new Vue({
    el: '#app',
    data() {
        return {
            result: [],
            loading: false,
            databases: [],
            error_status: '',
            error_message: '',
            form: {
                query: '',
                selected_features: [],
            },
            patterns: {
                query_patterns: [
                    {
                        value: 'competition_track = main_2020',
                        features: ['author', 'family', 'filename'],
                        text: "Main Track 2020"
                    },
                    {
                        value: 'competition_track = planning_2020',
                        features: ['author', 'family', 'filename'],
                        text: "Planning Track 2020"
                    },
                    {
                        value: 'competition_track = main_2019',
                        features: ['author', 'family', 'filename'],
                        text: "Main Track 2019"
                    },
                    {
                        value: 'filename like %waerden%',
                        features: [],
                        text: "Van Der Waerden Numbers"
                    },
                ],
                selected_pattern: undefined
            },
            table: {
                show: false,
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
            const host = this.getHost();
            $.ajax({
                url: host.concat("/listdatabases"),
                type: 'GET',
                dataType: 'json',
                success: function (result) {
                    for (let i = 0; i < result.length; i++) {
                        const url = host.concat("/listfeatures/".concat(result[i]));
                        $.ajaxSetup({async: false});
                        $.ajax({
                            url: url,
                            type: 'GET',
                            dataType: 'json',
                            success: function (features) {
                                app.databases.push([result[i], features, i]);
                            },
                            error: function (xhr, status, error) {
                                app.table.show = false;
                                app.error_status = xhr.statusText;
                                app.error_message = xhr.responseText;
                                app.showErrorModal();
                            }
                        });
                        $.ajaxSetup({async: true});
                    }
                },
                error: function (xhr, status, error) {
                    app.table.show = false;
                    app.error_status = xhr.statusText;
                    app.error_message = xhr.responseText;
                    app.showErrorModal();
                }
            })
        },
        submitQuery: function (event) {
            this.table.show = true;
            this.table.table_busy = true;

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
                    app.table.rows = result.length;
                    var entry = result[0];
                    for (var attribute in entry) {
                        app.table.fields.push({key: attribute.toString(), sortable: true});
                    }
                    app.table.table_busy = false;
                },
                error: function (xhr, status, error) {
                    app.table.table_busy = false;
                    app.table.show = false;
                    app.error_status = xhr.statusText;
                    app.error_message = xhr.responseText;
                    app.showErrorModal();
                }
            });
            if (event !== undefined) {
                event.preventDefault();
            }
        },
        showErrorModal() {
            this.$refs['error-modal'].show()
        },
        hideErrorModal() {
            this.error_message = '';
            this.$refs['error-modal'].hide()
        },
        init() {
            this.getDatabases();
            this.patterns.selected_pattern = this.patterns.query_patterns[0];
        },
        selectPattern() {
            this.form.selected_features = this.patterns.selected_pattern.features;
            this.form.query = this.patterns.selected_pattern.value;
        }
    },
    mounted() {
        this.init();
    },
    computed: {
        rows() {
            return this.result.length
        },
        query: {
            get: function () {
                return this.form.query;
            },
            set: function (newValue) {
                this.form.query = newValue;
            }
        },
        selected_features: {
            get: function () {
                return this.form.selected_features;
            },
            set: function (newValue) {
                this.form.selected_features = newValue;
            }
        },
    }
});
window.onload = function () {
    app.submitQuery();
}