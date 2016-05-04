# BOP Site

This is the BOP site (PHP Version) using [Slim](http://www.slimframework.com/) microframework.

## Install

### PHP

We recommend to use [PHP PPA](https://launchpad.net/~ondrej/+archive/ubuntu/php) to install PHP. You should have `apt-get` installed first.

Since PHP 7.0 is nearly twice as fast as PHP 5.6, we recommend to install the latest 7.0 version.

```bash
sudo add-apt-repository ppa:ondrej/php
sudo apt-get update
sudo apt-get install php7.0 php7.0-cli php7.0-fpm php7.0-curl php7.0-json php7.0-mysql
```

*Note: If you have installed PHP 5.x, it is recommended to remove it first.*

### Dependencies

To configure this site, you need to install [Composer](http://www.phpcomposer.com/) first. Composer is a package manager for PHP packages.

After Composer is installed, execute the following commands in `site` directory:

```bash
composer install
```

Or:

```bash
php composer.phar install
```

A `vendor` directory should appear in your `site` directory.

*Note: If Composer asks you for GitHub Access token, create one in your GitHub profile settings and give that token back to Composer.*

### Server

A simple way to start this site is to run the following command:

```bash
php -S localhost:8000 -t ./site/public
```

For performance reasons, we need to configure it with nginx. Here is the [PPA Repository](https://launchpad.net/~nginx/+archive/ubuntu/stable).

```bash
sudo add-apt-repository ppa:nginx/stable
sudo apt-get update
sudo apt-get install nginx
```

Create a file `bop-site` in `/etc/nginx/sites-available`:

```
server {
    listen 80;              # Or other port
    server_name localhost;  # Or other domain
    index index.php;
    error_log /path/to/example.error.log;    # Error log
    access_log /path/to/example.access.log;  # Access log
    root /path/to/public;                    # Our site path

    location / {
        try_files $uri $uri/ /index.php$is_args$args;
        autoindex off;
    }

    location ~ \.php {
        try_files $uri =404;
        fastcgi_split_path_info ^(.+\.php)(/.+)$;
        include fastcgi_params;
        fastcgi_param SCRIPT_FILENAME $document_root$fastcgi_script_name;
        fastcgi_param SCRIPT_NAME $fastcgi_script_name;
        fastcgi_index index.php;
        fastcgi_pass 127.0.0.1:9000;
    }
}
```

*Note: For security reasons, PHP-FPM may not enable port mode by default. To enable it, check the config file in `/etc/php/7.0/fpm/pool`. Or you can use UNIX socket to configure FastCGI.*

Create a symbol link in `sites-enabled`:

```bash
cd /etc/nginx/sites-enabled
sudo ln -s ../sites-available/bop-site bop-site
sudo nginx -t # Check configuration file
```

If you want to make theses steps easier, just create `bop-site.conf` in `/etc/nginx/conf.d`. No symbol links are needed if you follow this way.

Then restart nginx:
```bash
sudo service nginx restart
```

If all things are OK, you should see the site work at `http://localhost:80/`.
