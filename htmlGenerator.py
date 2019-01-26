from flask import url_for


def generate_html_header(language):
    css = url_for('static', filename='bootstrap/css/bootstrap.min.css')
    response = "<!DOCTYPE html>\n<html lang=\"{}\">\n" \
               "<head>\n    <meta charset=\"utf-8\">\n" \
               "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1, shrink-to-fit=no\">\n" \
               "    <link rel=\"stylesheet\" " \
               "href=\"{}\">\n" \
               "</head>\n".format(language, css)
    return response


def generate_head(title):
    response = "<div class=\"page-header\">\n" \
               "    <h1>{}</h1>\n" \
               "</div>\n".format(title)
    return response


def generate_num_table_div(entries):
    counter = 0
    response = "<div style=\"display: block; max-height: 600px; overflow-y: auto; " \
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
    response = "<div style=\"display: block; max-height: 600px; overflow-y: auto; " \
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
                    "           </tr>\n".format(entry[0], *entry[1])
    response += "       </tbody>\n" \
                "   <table>\n" \
                "</div>\n"
    return response


def generate_warning(text):
    return "<div class=\"alert alert-warning\"> " \
                "<strong>Warning!</strong> {}" \
                "</div>".format(text)


def generate_zip_download(path):
    return "<div class=\"container\"><a href=\"{}\" download>Download zip</a></div>".format(path)
