#!/usr/bin/env php
<?php

require __DIR__.'/vendor/autoload.php';
include_once __DIR__.'/config.inc.php';

$debug = false;
if(defined('DEBUG_MODE') || (isset($argv[1]) && $argv[1] == '-d'))
    $debug = true;

$beacon = new BPX\Beacon(BPX_HOST, BPX_PORT, BPX_CRT, BPX_KEY);
$pdo = NULL;

while(true) {
    try {
        if($debug) echo "Next iteration\n";
        
        $pdo = new PDO('mysql:host='.DB_HOST.';dbname='.DB_NAME, DB_USER, DB_PASS);
        $pdo -> setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $pdo -> setAttribute(PDO::ATTR_EMULATE_PREPARES, false);
        $pdo -> setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_ASSOC);
        
        $pdo -> query('CREATE TABLE IF NOT EXISTS blocks(
                           height bigint not null primary key,
                           hash varchar(66) not null,
                           timestamp bigint default null,
                           coinbase varchar(42) not null,
                           body longtext not null,
                           execution_block_hash varchar(66) default null,
                           fee_recipient varchar(42) default null,
                           wd_addresses text default null
                       )');
        $pdo -> query('CREATE TABLE IF NOT EXISTS state(
                           network_name varchar(128) not null primary key,
                           peak_height bigint not null,
                           difficulty bigint not null,
                           netspace bigint not null
                       )');
        
        $networkInfo = $beacon -> getNetworkInfo();
        $blockchainState = $beacon -> getBlockchainState();
        
        $task = [
            ':network_name' => $networkInfo -> network_name,
            ':peak_height' => $blockchainState -> peak -> height,
            ':difficulty' => $blockchainState -> difficulty,
            ':netspace' => $blockchainState -> space
        ];
        
        $sql = 'REPLACE INTO state(
                    network_name,
                    peak_height,
                    difficulty,
                    netspace
                )
                VALUES(
                    :network_name,
                    :peak_height,
                    :difficulty,
                    :netspace
                )';
        
        $q = $pdo -> prepare($sql);
        $q -> execute($task);
        
        if($debug) echo "Updated state\n";
        
        // Step 1. Get height and hash of the latest block in database
        // Set -1, NULL if database empty
        $dbHeight = -1;
        $dbHash = NULL;
        $q = $pdo -> query("SELECT height, hash FROM blocks ORDER BY height DESC LIMIT 1");
        $row = $q -> fetch();
        if($row) {
            $dbHeight = $row['height'];
            $dbHash = $row['hash'];
        }
        if($debug) echo "DB height: $dbHeight\nDB hash: $dbHash\n";
        
        // Step 2. If at least 1 block exists in database, check for potential reorg
        // by backwards fetching node blocks by height and comparing node block hash to database
        // block hash. If the hash matches, there was no reorg, if the hash differs, check previous block
        
        if($dbHeight >= 0) {
            if($debug) echo "Checking for reorgs\n";
            
            while(true) {
                $record = $beacon -> getBlockRecordByHeight($dbHeight);
                if($debug) echo "Height = $dbHeight, DB hash = $dbHash, node hash = ".$record -> header_hash." ";
                if($record -> header_hash == $dbHash) {
                    if($debug) echo "(good)\n";
                    break;
                }
                if($debug) echo "(reorg)\n";
                $dbHeight--;
                if($dbHeight == -1)
                    break;
                $q = $pdo -> prepare("SELECT hash FROM blocks WHERE height = :height");
                $q -> execute([':height' => $dbHeight]);
                $row = $q -> fetch();
                if(!$row)
                    throw new Exception('Block expected in database but not available');
                $dbHash = $row['hash'];
            }
        }
        
        // Step 3. Start fetching and adding/replacing blocks to database from dbHeight + 1 to the latest blocks
        // known by node
        while(true) {
            $dbHeight++;
            
            $record = $beacon -> getBlockRecordByHeight($dbHeight);
            $block = $beacon -> getBlock($record -> header_hash);
            
            $jsonBlock = json_encode($block, JSON_UNESCAPED_SLASHES);
            
            $task = [
                ':height' => $dbHeight,
                ':hash' => $record -> header_hash,
                ':coinbase' => $record -> coinbase,
                ':body' => $jsonBlock,
                ':timestamp' => NULL,
                ':execution_block_hash' => NULL,
                ':fee_recipient' => NULL,
                ':wd_addresses' => NULL
            ];
            
            if(isset($record -> timestamp))
                $task[':timestamp'] = $record -> timestamp;
            
            if(isset($record -> execution_block_hash))
                $task[':execution_block_hash'] = $record -> execution_block_hash;
            
            if(isset($block -> execution_payload -> feeRecipient))
                $task[':fee_recipient'] = $block -> execution_payload -> feeRecipient;
            
            if(!empty($block -> execution_payload -> withdrawals)) {
                $task[':wd_addresses'] = '';
                foreach($block -> execution_payload -> withdrawals as $wd) {
                    if($task[':wd_addresses'] != '') $task[':wd_addresses'] .= ',';
                    $task[':wd_addresses'] .= $wd -> address;
                }
            }
            
            $sql = 'REPLACE INTO blocks(height, hash, coinbase, body, timestamp, execution_block_hash, fee_recipient, wd_addresses)
                    VALUES(:height, :hash, :coinbase, :body, :timestamp, :execution_block_hash, :fee_recipient, :wd_addresses)';
                    
            $q = $pdo -> prepare($sql);
            $q -> execute($task);
            
            if($debug) echo "Inserted block: height = $dbHeight\n"; 
        }
    }
    
    catch(Exception $e) {
        echo get_class($e).': '.$e->getMessage()."\n";
    }
    
    unset($pdo);
    sleep(5);
}

?>
