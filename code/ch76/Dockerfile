# Chapter 45: the book's image. The official 4.3.0 Python image, plus the PyArrow and pandas that
# Arrow UDFs need on the executors (Chapter 42), plus the chapter's application.
FROM apache/spark:4.3.0-python3
USER root
RUN pip3 install --no-cache-dir "pyarrow==18.1.0" "pandas==2.2.3"
COPY app/ /opt/book/
USER spark
