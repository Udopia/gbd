from flask import render_template, json


def render_start_page(groups):
    return render_template('quick_search.html', groups=groups, is_result=False, has_query=False)


def render_result_page(groups, results, checked_groups, query):
    return render_template('quick_search_content.html',
                           groups=groups,
                           is_result=True,
                           is_warning=False,
                           is_zip=False,
                           results=results,
                           results_json=json.dumps(results),
                           checked_groups=checked_groups,
                           checked_groups_json=json.dumps(checked_groups),
                           has_query=(query != ""),
                           query=query)


def render_warning_page(groups, checked_groups, warning_message, query):
    return render_template('quick_search_content.html',
                           groups=groups,
                           is_result=False,
                           is_warning=True,
                           is_zip=False,
                           checked_groups=checked_groups,
                           checked_groups_json=json.dumps(checked_groups),
                           warning_message=warning_message,
                           has_query=(query != ""),
                           query=query)


def render_zip_reload_page(groups, checked_groups, zip_message, query):
    return render_template('quick_search_content.html',
                           groups=groups,
                           is_result=False,
                           is_warning=False,
                           is_zip=True,
                           checked_groups=checked_groups,
                           checked_groups_json=json.dumps(checked_groups),
                           zip_message=zip_message,
                           has_query=(query != ""),
                           query=query)
