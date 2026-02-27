<?php

namespace App;

use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        ini_set('memory_limit', '4G');

        $result = [];

        $handle = fopen($inputPath, 'r');
        while (($line = fgets($handle)) !== false) {
            [$url, $date] = explode(',', trim($line));
            $path = parse_url($url, PHP_URL_PATH);
            $day = substr($date, 0, 10);
            $result[$path][$day] = ($result[$path][$day] ?? 0) + 1;
        }
        fclose($handle);

        foreach ($result as &$days) {
            ksort($days);
        }

        file_put_contents($outputPath, json_encode($result, JSON_PRETTY_PRINT));
    }
}