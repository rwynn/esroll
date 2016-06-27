# esroll
a go daemon to manage your elasticsearch indices

<img width="480" height="339" src="https://raw.github.com/rwynn/esroll/master/images/esroll.jpg"/>

### Install ###

	go get github.com/rwynn/esroll
	
### How is this different from elastic curator? ###

[Curator](https://github.com/elastic/curator) is a tool from elastic for running [Actions](https://www.elastic.co/guide/en/elasticsearch/client/curator/current/actions.html) on your indices.  Actions take arguments which let you 

  * filter and select the set on indices to operate on
  * customize the options for the action performed on the selected indices.

Curator Actions can be aggregated in [Action Files](https://www.elastic.co/guide/en/elasticsearch/client/curator/current/actionfile.html) to perform higher level operations.  Well, of all the possible combinations of Actions you could do with curator in a Action File, esroll wires a series of targeted Actions which it calls a `roll`.  

The job of a roll is to create a new index, adjust the set of indices which a pair of aliases point to, and finally perform the following operations on old indices: update settings, optimize (force merge), close, or delete.  So, in contrast to curator, esroll is not Action based.  You don’t aggregate Actions in an Action File to give to esroll, rather you tell esroll what events trigger a `roll` and declare how the roll is performed.  Events can be temporal, like run a roll every 2 hours, or events can be based on changes in attributes of the index (i.e. physical size), like run a roll when an index exceeds 2GB.

### Design ###

esroll is a go daemon to ensure some elasticsearch scaling best practices such as
[Index per Time Frame](https://www.elastic.co/guide/en/elasticsearch/guide/current/time-based.html) and
[Retiring Data](https://www.elastic.co/guide/en/elasticsearch/guide/current/retiring-data.html).

esroll helps you manage indices by keeping a pair of aliases (one for indexing and one for search)
pointing to a set of indices it creates periodically. At a minimum you need to configure esroll to have an `indexTarget`
and a `rollUnit`.  

Say, for example, that you configure esroll with an indexTarget of `logs`, a rollUnit of `years`, and set searchAliases 
to `2`. esroll would begin by checking the existence of an index `logs_2016`.  If the index does not exist, esroll would 
create it and assign the aliases `logs` and `logs_search`. The former alias is the one meant for indexing new data
and the latter is meant for searching data. This gives you the following index:

	logs_2016 -> aliases logs and log_search

In daemon mode esroll will continue running and wait until the year rolls over to 2017. At the point esroll would
do the following:

Check for the existence of an index `logs_2017`.  If the index does not exist, esroll will create it and then
assign the aliases `logs` and `logs_search`.  Since there should only ever be one real index backing the `logs`
alias (the most recent), esroll will remove the `logs` alias from the `logs_2016` index.  Since the `searchAliases` 
option is set to 2 and there are equally 2 time based indices at this point, esroll will keep the `logs_search` 
alias on the `logs_2016` index.

At that point you would have the following indices:

	logs_2017 -> aliases logs and logs_search
	logs_2016 -> alias logs_search

When you index into `logs` you are actually indexing into the time based index for 2017.  And when you
query `logs_search` you are actually searching 2 time based indices - one for 2016 and one for 2017.

When an index goes from having both aliases `logs` and `logs_search` to having only the search alias `logs_search`,
you can tell esroll to optimize the index by using the `optimizeOnRoll` setting.  When indices go from indexing and
searching to only searching (readonly) you can get peformance gains on search by doing an optimization to reduce the segments
within the index. The max number of segments to optimize to is also available as a setting.

This rolling over algorithm would continue happening each time the year changes. In this example, when the year
turns 2018, you would have the following indices:

	logs_2018 -> aliases logs and logs_search
	logs_2017 -> alias logs_search
	logs_2016 -> alias logs_search

At this point some other esroll settings come into play.  Since the setting searchAliases was set to 2, and the
alias `logs_search` currently points to 3 indices, esroll needs to fix this.  It will remove the alias
`logs_search` from `logs_2016`.  Thus, you get the following:

	logs_2018 -> aliases logs and logs_search
	logs_2017 -> alias logs_search
	logs_2016 

Esroll lets you configure what you want to do with indices which no longer have an alias.  You can configure
esroll to delete them, close them, or just keep them open. 

In this example a `rollUnit` of years was used to keep things simple.  But rolling over once a year is probably not
optimal if you have alot of data coming into elasticsearch.  esroll provides the following values for `rollUnit` - minutes,
hours, days, months, years, and bytes.  esroll provides another option `rollIncrement` which is an integer. Together `rollUnit`
and `rollIncrement` allow you to tell esroll to run its algorithm at intervals like 20 minutes, 3 hours, or 5 months.

When something like 3 hours is used, its important to understand that esroll does not necessary roll 3 hours from the last
roll but rather when the hour of the day % 3 == 0.  It's possible that if you start esroll with this setting 
at 1am, esroll would do an initial roll at 1am and then again at 3am, 6am, 9am, etc.  So even though you had set to roll
every 3 hours, due to index initialization, there is only 2 hours between the 1st and 2nd roll.  

From an elasticsearch client perspective you would usually deal only with the pair of indexes aliases created by
esroll and not the time based raw indexes.  This allows your client code concern itself with logical index names (indexing
and searching) even though the indexes backing those aliases are changing over time.  

Finally, the way the algorithm is explained above it may lead you to think that alias updates at the time of a roll are done
serially, however, this is not the case.  All the alias updates are gathered together on the roll and made in one 
request to elasticsearch.  

### Usage ###

Before running esroll you will probably want to configure it. It's not actually required that you config esroll before
running it though.  That's because the esroll configuration is stored in elasticsearch and esroll will poll
periodically for changes in its configuration.

Configuring esroll is done by indexing documents into the `esroll` index with the type `config`.  The following is an
example of how to get a configuration into elasticsearch...

	curl -XPUT localhost:9200/esroll/config/snowball -d '{
		"targetIndex": "snowball",
		"rollUnit": "minutes",
		"rollIncrement": 3,
		"searchAliases": 4,
		"searchSuffix": "search",
		"deleteOld": false,
		"closeOld": true,
		"optimizeOnRoll": true,
		"optimizeMaxSegments": 2,
		"settings": {
			"index.routing.allocation.include.box_type" : "strong",
			"index": {
				"number_of_replicas": 1
			}
		},
		"settingsOnRoll": {
			"index.routing.allocation.include.box_type" : "medium"
		}
	}'

The example above creates a document describing one rolling index that esroll will manage. The most important parts of
this configuration are the `targetIndex` and `rollUnit`.  The `targetIndex` is not actually required, though.  If you
omit it, the `targetIndex` will default to the ID of the configuration document.  So in this case `targetIndex` is
redundant since the ID of the configuration document is the same.  The `rollUnit` setting tells esroll the time unit
for which you want to roll.  The `rollIncrement` setting is combined with the `rollUnit` setting to further detail
when you want the roll to occur.  If `rollIncrement` is not supplied it defaults to `1`.  

So above we are telling esroll to run its roll algorithm every time clock seconds == 0 and clock minute % 3 == 0.  If
we ran esroll at 1:00PM GMT it would immediatly run it's roll algorithm and create the index `snowball_2016-05-31-13-00`
and give that index aliases `snowball` and `snowball_search`.  Then at 1:03PM GMT, 1:06PM GMT, and so on, it would
perform a roll - create a new timestamped index and adjust the aliases.

Let's look at some of the other settings we have configured...

	searchAliases = 4 -- keep up to 4 recent indexes with the search alias
	searchSuffix = search -- suffix the search alias names with 'search'
	deleteOld = false -- do not delete indices when the alias count for the index drops to 0
	closeOld = true -- flush and close indices when the alias count for the index drops to 0
	optimizeOnRoll = true -- optimize the index when the alias count for the index drops to 1 (search)
	optimizeMaxSegments = 2 -- pass max_num_segments=2 when optimizing
	settings = elasticsearch settings -- use the specificed settings when creating each new time based index
	settingsOnRoll = elasticseach settings -- update the index settings with this when the alias count for the index drops to 1 (search)

You may have as many of these configuration documents as you would like.  esroll will find them (even when running)
and start using them.

`rollUnit` is the only configuration option which is required.  the defaults for missing options are:

	targetIndex          : the ID of the configuration document
	rollIncrement        : 1
	searchAliases        : 2
	searchSuffix         : "search"
	deleteOld            : false
	closeOld             : false
	optimizeOnRoll       : false
	optimizeMaxSegments  : do not specify max segments when optimizing
	settings             : do not specify settings when creating indexes
	settingsOnRoll       : do not update settings on roll

You start esroll as follows:

	esroll \[-url ES-REST-URL\] \[-daemon true|false\] \[-pem PATH_TO_PEM_FILE\]

esroll can be run in 2 modes: single shot and daemon mode.  The single shot mode is the default.  In that mode esroll
will pull down all the settings documents, run a single roll on each of them and quit.  Use this mode if you wish to
externally control when rolls are peformed (such as via a cron job).  The daemon mode is enabled by supplying `-daemon`
on the command line.  In this mode esroll will manage scheduling internally.  It will run until it is explicitly stopped
and periodically roll indexes according to the settings, updating its configuration dynamically along the way by pulling
the most recent versions of the configuration documents.

By default esroll will expect the elasticsearch REST API to be available at http://localhost:9200.  If you need to change
this supply the `-url` argument and specify the URL to the elasticsearch REST API.

If you need to install a self-signed certificate for connections to the elasticsearch REST API you can do so using 
the `-pem` argument with the path to your PEM file.

### Size Based Indexes ###

A unique feature of esroll is that it supports size based indices.  That is you can configure esroll to run its roll
algorithm when your primary index reaches a certain number of bytes on disk. esroll uses the cat indices API of elasticsearch
to get the size on disk of the index periodically and rolls if it exceeds the configured threshold.

To configure esroll for size based indices you would set the rollUnit to `bytes` and set the option `rollSize` to a human
readable string representing the maximum size you would like the primary index to grow to. esroll uses the
[go-humanize](https://github.com/dustin/go-humanize) library to parse the human readable size that you set on `rollSize`.

For example, to test this feature you could set a low threshold of 20KB like so:

	{
		"targetIndex": "snowball",
		"rollUnit": "bytes",
		"rollSize": "20KB",
		"searchAliases": 4,
		"searchSuffix": "search",
		"deleteOld": false,
		"closeOld": true,
		"optimizeOnRoll": true,
		"settings": {
			"index": {
				"number_of_replicas": 1
			}
		}
	}

You can verify the size on disk of indices by using the cat index API.  For example:

	$ curl localhost:9200/_cat/indices/_all
	yellow open esroll                       5 1 1 0  6.2kb  6.2kb
	yellow open snowball_2016-06-03-17-38-53 5 1 4 0 22.4kb 22.4kb
	yellow open snowball_2016-06-03-17-36-53 5 1 4 0 22.4kb 22.4kb
	yellow open snowball_2016-06-03-17-39-23 5 1 0 0   795b   795b
	yellow open snowball_2016-06-03-17-35-03 5 1 4 0 22.4kb 22.4kb

Since the size based roll is triggered by a short timeout it's possible that data is able to sneak in before the size check
occurs.  You can under-size your `rollSize` value if this is the case to get closer to the desired index size.

