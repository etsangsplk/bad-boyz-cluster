

function console(){
	var _ = this;


	function do_updates(){
		update();
		setTimeout( function() { do_updates(); }, 1500 )
	}

	function node_id(n){
		return (n.ip_address + "-" + n.port).replace(/\./g, '_');
	} 
	function node_name(n){
		return n.ip_address + ":" + n.port;
	} 

	function render(updates){
		// 

		for (var i=0; i < updates.length; i++){
			// Check to see if we already have a node here?
			var n = updates[i];


			var li = $("#" + node_id(n) );
			if ( li.length==0){
				// Create a new list item for you please!
				li = $("#node-template").clone();
				li.attr("id", node_id(n));
				$("#nodes").append( li );
			}

			li.find(".node-name").text(node_name(n));
			li.find(".cpu-value").text( parseInt(n.cpu) + "%" );
			li.find(".job-value").text( n.current_job );
		}
	}


	function update(){
		$.ajax({
			url: "/json/nodes",
			success: function (response) {
				render(response.nodes);
			},
			error: function () {
			}
		});
	}

	do_updates();


}

