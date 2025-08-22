FROM nginx:alpine

# Install apache2-utils for htpasswd
RUN apk add --no-cache apache2-utils

# Copy nginx configuration
COPY nginx.conf /etc/nginx/nginx.conf

# Create htpasswd file for Airflow admin access
RUN htpasswd -cb /etc/nginx/.htpasswd admin airflow123

# Copy frontend build files (will be mounted as volume in production)
COPY frontend/dist /usr/share/nginx/html

# Create log directory
RUN mkdir -p /var/log/nginx

# Expose port 80
EXPOSE 80

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost/health || exit 1

CMD ["nginx", "-g", "daemon off;"]