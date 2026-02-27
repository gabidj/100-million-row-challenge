<?php

namespace App;

use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        ini_set('memory_limit', '4G');

        $numWorkers = 8;
        $fileSize = filesize($inputPath);
        $chunkSize = intdiv($fileSize, $numWorkers);

        $tmpFiles = [];
        $pids = [];

        for ($i = 0; $i < $numWorkers; $i++) {
            $tmpFile = tempnam(sys_get_temp_dir(), 'parser_');
            $tmpFiles[] = $tmpFile;

            $start = $i * $chunkSize;
            $end = ($i === $numWorkers - 1) ? $fileSize : ($i + 1) * $chunkSize;

            $pid = pcntl_fork();
            if ($pid === -1) {
                die('Could not fork');
            } elseif ($pid === 0) {
                // Child process
                $result = [];
                $handle = fopen($inputPath, 'r');

                if ($start > 0) {
                    fseek($handle, $start);
                    fgets($handle); // skip partial line
                }

                while (ftell($handle) < $end && ($line = fgets($handle)) !== false) {
                    [$url, $date] = explode(',', trim($line));
                    $path = parse_url($url, PHP_URL_PATH);
                    $day = substr($date, 0, 10);
                    $result[$path][$day] = ($result[$path][$day] ?? 0) + 1;
                }
                fclose($handle);

                file_put_contents($tmpFile, json_encode($result));
                exit(0);
            } else {
                $pids[] = $pid;
            }
        }

        // Wait for all children
        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        // Merge results
        $result = [];
        foreach ($tmpFiles as $tmpFile) {
            $partial = json_decode(file_get_contents($tmpFile), true);
            unlink($tmpFile);
            foreach ($partial as $path => $days) {
                foreach ($days as $day => $count) {
                    $result[$path][$day] = ($result[$path][$day] ?? 0) + $count;
                }
            }
        }

        foreach ($result as &$days) {
            ksort($days);
        }

        file_put_contents($outputPath, json_encode($result, JSON_PRETTY_PRINT));
    }
}