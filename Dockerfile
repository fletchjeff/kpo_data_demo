FROM quay.io/astronomer/astro-runtime:7.3.0

# this patch is here until this gets merged into the 
# astro runtime https://github.com/apache/airflow/pull/28942
COPY temp_fixes/cncf/ /usr/local/lib/python3.9/site-packages/airflow/providers/cncf/


# This section is used to provide authentication to the EKC cluster
COPY --chown=astro:astro include/.aws /home/astro/.aws
COPY --chown=astro:astro include/config /home/astro/config
ENV CLUSTER_CONTEXT=arn:aws:eks:eu-central-1:559345414282:cluster/jf-eks