<?php
$cat="welcome";
if (isset($_GET["cat"])) {
  $cat=$_GET["cat"];
}
?>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>GBD - Global Benchmark Database</title>
<link rel="stylesheet" href="main.css" type="text/css">
<link rel="icon" type="image/x-icon" href="icon.ico">
</head>
<body>

<div class="main">
  
  <div class="navigation">
    <img src="logo.png" border="0" width="180" alt="GBD">
    <br /><br />
    <a href="index.php?cat=start" <?php if ($cat == "start") echo "class='active'"; ?>>Start</a>
    <br />
    <a href="index.php?cat=links" <?php if ($cat == "links") echo "class='active'"; ?>>Links</a>
    <br />
  </div>

  <div class="content">
    <?php
    $cats = array(
      "start", "links"
    );

    if (in_array($cat, $cats)) {
      include($cat.".html");
    } else {
      include("start.html");
    }
    ?>
  </div>
    
</div>

</body>
</html>
