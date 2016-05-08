<?php
use \Psr\Http\Message\ServerRequestInterface as Request;
use \Psr\Http\Message\ResponseInterface as Response;

require '../vendor/autoload.php';

$app = new \Slim\App;
$app->any('/', function (Request $request, Response $response) {
    // Input from request
    $id1 = $request->getQueryParam('id1', 0);
    $id2 = $request->getQueryParam('id2', 0);

    // Process Data......
    // See [Guzzle](http://docs.guzzlephp.org/en/latest/) for how to send requests synchronously or asynchronously

    $result = [];   // Empty array

    // Return processed result
    return $response->withJson($result);
});
$app->run();
