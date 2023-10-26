from flask import Flask, render_template, request, send_from_directory, redirect, url_for
import os
from utils import separate_data, work_200_005, work_8186
import utils_multi
import time
from concurrent.futures import ProcessPoolExecutor,ThreadPoolExecutor

app = Flask(__name__)


@app.route('/', methods=["post", "get"])
def home():  # put application's code here
    print(request.files)
    if request.method == "POST":
        if len(request.files) < 2:
            return '<h1>Please upload all files</h1>'

        hisense_file = request.files['hisense']
        qls_file = request.files['qls']

        if hisense_file.filename == '' or qls_file.filename == '':
            return 'No selected file.'

        if hisense_file and qls_file:
            h_filename = os.path.join('static', "hisense_file.xlsx")
            hisense_file.save(h_filename)
            q_filename = os.path.join('static', "qls_file.xlsx")
            qls_file.save(q_filename)
            return redirect(url_for("result"))
    return render_template("home.html")


@app.route('/result')
def result():
    start = time.time()
    separate_data()


    work_200_005()


    work_8186()


    file_names_200 = os.listdir("./static/h_200_q_005")
    file_paths_200 = [os.path.join("..", "static", "h_200_q_005", f) for f in os.listdir("./static/h_200_q_005")]

    file_names_8186 = os.listdir("./static/h_8186_q_8186")
    file_paths_8186 = [os.path.join("..", "static", "h_8186_q_8186", f) for f in os.listdir("./static/h_8186_q_8186")]

    file_names_mess = os.listdir("./static/h_blank_q_mess")
    file_paths_mess = [os.path.join("..", "static", "h_blank_q_mess", f) for f in os.listdir("./static/h_blank_q_mess")]

    info = {
        "8186 Files": {"name": file_names_8186, "path": file_paths_8186},
        "200 Files": {"name": file_names_200, "path": file_paths_200},
        "Mess Files": {"name": file_names_mess, "path": file_paths_mess}
    }
    # print(info)
    end = time.time()
    print("Separate original data:", end - start)

    return render_template("result.html", info=info)


@app.route('/result_update')
def result_update():
    start = time.time()
    df_h_8186, df_h_200, df_h_blank, df_q_8186, df_q_005, df_q_mess = utils_multi.separate_data()

    with ProcessPoolExecutor(max_workers=4) as executor:
        # Submit each function to the executor and get a Future object
        future_8186 = executor.submit(utils_multi.match_list_8186, df_h_8186, df_q_8186)
        future_200 = executor.submit(utils_multi.match_list_200, df_h_200, df_q_005)

        # Retrieve results from Future objects
        match_list_8186 = future_8186.result()
        match_list_200 = future_200.result()

    with ProcessPoolExecutor(max_workers=4) as executor:
        # Submit each function to the executor and get a Future object
        future_h_8186 = executor.submit(utils_multi.work_h_8186, df_h_8186, match_list_8186)
        future_q_8186 = executor.submit(utils_multi.work_q_8186, df_q_8186, match_list_8186)
        future_h_200 = executor.submit(utils_multi.work_200_005_h, df_h_200, match_list_200)
        future_q_200 = executor.submit(utils_multi.work_200_005_q, df_q_005, match_list_200)

        # Retrieve results from Future objects
        df_h_8186_match, df_h_8186_diff = future_h_8186.result()
        df_q_8186_match, df_q_8186_diff = future_q_8186.result()
        df_h_200_match, df_h_200_diff = future_h_200.result()
        df_q_005_match, df_q_005_diff = future_q_200.result()


    df_list = [
        df_h_8186,
        df_h_8186_match,
        df_h_8186_diff,
        df_q_8186,
        df_q_8186_match,
        df_q_8186_diff,
        df_h_200,
        df_h_200_match,
        df_h_200_diff,
        df_q_005,
        df_q_005_match,
        df_q_005_diff,
        df_h_blank,
        df_q_mess
    ]
    df_name_list = [
        "df_h_8186",
        "df_h_8186_match",
        "df_h_8186_diff",
        "df_q_8186",
        "df_q_8186_match",
        "df_q_8186_diff",
        "df_h_200",
        "df_h_200_match",
        "df_h_200_diff",
        "df_q_005",
        "df_q_005_match",
        "df_q_005_diff",
        "df_h_blank",
        'df_q_mess'
    ]
    folder_list = [
        "h_8186_q_8186",
        "h_8186_q_8186",
        "h_8186_q_8186",
        "h_8186_q_8186",
        "h_8186_q_8186",
        "h_8186_q_8186",
        "h_200_q_005",
        "h_200_q_005",
        "h_200_q_005",
        "h_200_q_005",
        "h_200_q_005",
        "h_200_q_005",
        "h_blank_q_mess",
        "h_blank_q_mess",
    ]

    for i in range(len(df_list)):
        path = os.path.join(".", "static", folder_list[i], df_name_list[i] + ".csv")
        df_list[i].to_csv(path)

    file_names_8186 = os.listdir("./static/h_8186_q_8186")
    file_paths_8186 = [os.path.join("..", "static", "h_8186_q_8186", f) for f in os.listdir("./static/h_8186_q_8186")]

    file_names_200 = os.listdir("./static/h_200_q_005")
    file_paths_200 = [os.path.join("..", "static", "h_200_q_005", f) for f in os.listdir("./static/h_200_q_005")]

    file_names_mess = os.listdir("./static/h_blank_q_mess")
    file_paths_mess = [os.path.join("..", "static", "h_blank_q_mess", f) for f in os.listdir("./static/h_blank_q_mess")]

    info = {
        "8186 Files": {"name": file_names_8186, "path": file_paths_8186},
        "200 Files": {"name": file_names_200, "path": file_paths_200},
        "Mess Files": {"name": file_names_mess, "path": file_paths_mess}
    }
    # print(info)
    end = time.time()
    print("write:", end - start)

    return render_template("result.html", info=info)


if __name__ == '__main__':
    # process_pool = ProcessPoolExecutor()
    # thread_pool = ThreadPoolExecutor()
    app.run()
