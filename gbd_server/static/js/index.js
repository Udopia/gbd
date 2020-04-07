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
        fields: [],
        table: {
            sortBy: null,
            sortDesc: false,
            table_busy: false,
            current_page: 1,
            per_page: 10,
            options: [
                {value: 10, text: "Show 10 per page"},
                {value: 20, text: "Show 20 per page"},
                {value: 30, text: "Show 30 per page"},
            ],
            head_variant: "dark",
        },
        loading: false,
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
                    app.fields = [];
                    app.table.sortBy = null;
                    app.table.sortDesc = false;
                    app.result = result;
                    var entry = result[0];
                    for (var attribute in entry) {
                        app.fields.push({key: attribute.toString(), sortable: true});
                    }
                    app.table.table_busy = false;
                },
                error: function (error) {
                    app.table.table_busy = false;
                    console.log('Error: '.concat(error.toString()));
                    alert('Something went wrong. Check the console for details.');
                }
            });
            event.preventDefault();
        },
        getCsvFile: function (event) {
            app.loading = true;
            var jsonData = {
                query: this.form.query,
                selected_groups: this.form.selected_groups,
            };
            $.ajax({
                url: this.getHost().concat("/exportcsv"),
                type: 'POST',
                data: JSON.stringify(jsonData),
                contentType: 'application/json; charset=utf-8',
                success: function (response, status, xhr) {
                    app.loading = false;
                    app.initializeDownload(response, status, xhr, window, document);
                },
                error: function (error) {
                    app.loading = false;
                    console.log('Error: '.concat(error.toString()));
                    alert('Something went wrong. Check the console for details.');
                }
            });
            event.preventDefault();
        },
        getUrlFile: function (event) {
            app.loading = true;
            var jsonData = {
                query: this.form.query,
                selected_groups: this.form.selected_groups,
            };
            $.ajax({
                url: this.getHost().concat("/getinstances"),
                type: 'POST',
                data: JSON.stringify(jsonData),
                contentType: 'application/json; charset=utf-8',
                success: function (response, status, xhr) {
                    app.loading = false;
                    app.initializeDownload(response, status, xhr, window, document);
                },
                error: function (error) {
                    app.loading = false;
                    console.log('Error: '.concat(error.toString()));
                    alert('Something went wrong. Check the console for details.');
                }
            });
            event.preventDefault();
        },
        initializeDownload: function (response, status, xhr, window, document) {
            const type = xhr.getResponseHeader("Content-Type");
            var blob = new Blob([response], {type: type});
            var fileName = xhr.getResponseHeader("filename");
            var link = document.createElement('a');
            var URL = window.URL || window.webkitURL;
            link.href = URL.createObjectURL(blob);
            link.download = fileName;
            link.click();
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