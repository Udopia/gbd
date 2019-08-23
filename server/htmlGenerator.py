from flask import url_for


def generate_html_header(language):
    css = url_for('static', filename='bootstrap/css/bootstrap.min.css')
    custom_css = url_for('static', filename='css/style.css')
    response = "<!DOCTYPE html>\n<html lang=\"{}\">\n" \
               "<head>\n    <meta charset=\"utf-8\">\n" \
               "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1, shrink-to-fit=no\">\n" \
               "    <link rel=\"stylesheet\" " \
               "href=\"{}\">\n" \
               "    <link rel=\"stylesheet\" " \
               "href=\"{}\">\n" \
               "</head>\n".format(language, css, custom_css)
    return response


def generate_head(title):
    response = "<hr>" \
               "<div class=\"container\"><div class=\"page-header\">\n" \
               "    <h1>{}</h1>\n" \
               "</div></div>\n".format(title)
    return response


def generate_num_table_div(entries):
    counter = 0
    response = "<div class=\"container\"style=\"display: block; max-height: 600px; overflow-y: auto; " \
               "-ms-overflow-style: -ms-autohiding-scrollbar;\">\n" \
               "    <table class=\"table table-bordered table-dark table-striped\">\n" \
               "        <thead>\n" \
               "            <tr>" \
               "                <th scope=\"col\">Number</th>\n" \
               "                <th scope=\"col\">File</th>\n" \
               "            </tr>\n" \
               "        </thead>\n"
    response += "       <tbody>\n"
    for entry in entries:
        response += "           <tr>" \
                    "               <th scope=\"row\">{}</th>\n" \
                    "               <td>{}</td>\n" \
                    "           </tr>\n".format(counter, entry)
        counter += 1
    response += "       </tbody>\n" \
                "   <table>\n" \
                "</div>\n"
    return response


def generate_resolve_table_div(entries):
    response = "<div class=\"container\" style=\"display: block; max-height: 600px; " \
               "overflow-y: auto; " \
               "-ms-overflow-style: -ms-autohiding-scrollbar;\">\n" \
               "    <table class=\"table table-bordered table-dark table-striped\">\n" \
               "        <thead>\n" \
               "            <tr>" \
               "                <th scope=\"col\">Group</th>\n" \
               "                <th scope=\"col\">Value</th>\n" \
               "            </tr>\n" \
               "        </thead>\n"
    response += "       <tbody>\n"
    for entry in entries:
        response += "           <tr>" \
                    "               <th scope=\"row\">{}</th>\n" \
                    "               <td>{}</td>\n" \
                    "           </tr>\n".format(entry[0], entry[1])
    response += "       </tbody>\n" \
                "   <table>\n" \
                "</div>\n"
    return response


def generate_warning(text):
    return "<div class=\"container alert alert-warning\"> " \
                "<strong>Warning!</strong> {}" \
                "</div>".format(text)


def generate_zip_busy_page(zipfile, size):
        url = '/static/resources/gbd_logo_small.png'

        response = generate_html_header("en")
        response += "<body>" \
                    "<nav class=\"navbar navbar-expand-lg navbar-dark bg-dark\">" \
                    "   <a href=\"/\" class=\"navbar-left\"><img style=\"max-width:50px\" src=\"{}\"></a>" \
                    "   <a class=\"navbar-brand\" href=\"#\"></a>" \
                    "   <button class=\"navbar-toggler\" type=\"button\" data-toggle=\"collapse\" " \
                    "       data-target=\"#navbarNavAltMarkup\"" \
                    "       aria-controls=\"navbarNavAltMarkup\" " \
                    "       aria-expanded=\"false\"" \
                    "       aria-label=\"Toggle navigation\">" \
                    "       <span class=\"navbar-toggler-icon\"></span>" \
                    "   </button>" \
                    "   <div class=\"collapse navbar-collapse\" id=\"navbarNavAltMarkup\">" \
                    "       <div class=\"navbar-nav\">" \
                    "           <a class=\"nav-item nav-link\" href=\"/\">Home</a>" \
                    "           <a class=\"nav-item nav-link\" href=\"/groups/all\">Groups" \
                    "           <a class=\"nav-item nav-link active\" href=\"/query/form\">Search</a>" \
                    "                   <span class=\"sr-only\">(current)</span></a>" \
                    "           <a class=\"nav-item nav-link\" href=\"/resolve/form\">Resolve</a>" \
                    "       </div>" \
                    "   </div>" \
                    "</nav>" \
                    "<hr>".format(url)
        response += '<div class=\"container bg-dark text-white\">' \
                    '   <div class="container mx-auto text-center">' \
                    '       <img src=\"{}\"' \
                    '       class=\"img-responsive\" alt=\"Logo\">' \
                    '   </div>' \
                    '   <h1>Query</h1>'.format(url)
        response += "<div class=\"container\">" \
                    "<a href=\"/zips/busy?file={}\">Creating ZIP for you. " \
                    "Click to check if zip has been created yet</a>" \
                    "<hr>" \
                    "<a>The size is approximately {} MB." \
                    "<hr>" \
                    "<progress value=\"0\" max=\"100\"></progress>" \
                    "</div>" \
                    "<hr>" \
                    "</div>".format(zipfile, size)
        return response
