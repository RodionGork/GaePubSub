$(document).ready(function() {
    updateTopicList();
});

function updateTopicList() {
    $.getJSON('/demo?q=topics', function(data) {
        var select = $('#topics');
        select.empty();
        for (var i in data.topics) {
            var name = data.topics[i];
            $('<option>' + name + '</option>').attr('value', name).appendTo(select);
        }
    });
}

function publishData() {
    var topic = $('#topics').val();
    var data = $('#data').val();
    $.post('/demo?topic=' + topic, data, function() {alert('Ok!')}, 'text');
}

function createTopic() {
    var name = prompt("New Topic Name");
    if (name == null) {
        return;
    }
    $.getJSON('/demo?q=topic&name=' + name, function(data) {
        alert(data.ok ? "Topic was created!" : "Some error had happened");
        updateTopicList();
    });
}

function deleteTopic() {
    var topic = $('#topics').val();
    var ur = confirm("Delete topic '" + topic + "' - are you sure?");
    if (!ur) {
        return;
    }
    $.getJSON('/demo?q=topic-del&name=' + topic, function(data) {
        alert(data.ok ? "Topic was deleted!" : "Some error had happened");
        updateTopicList();
    });
}