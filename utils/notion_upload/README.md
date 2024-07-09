# Notion upload tools

This is just a few scripts to scrape all our art out of notion, and upload it to hoardbooru.
It's easier to find things in hoardbooru, more pleasant to browse.
Though, I'm not sure how non-image content will work, or PSD files and things.


The plan is:
- Download stuff from notion
- Check for duplicate on hoardbooru, flag if there is a suggested duplicate
- Convert some tags
- - Mark artist tags as artist type
- - Mark character tags as character type
- - Flag up owner tags, maybe?
- - Mark status:final or status:wip as appropriate
- - Mark relations between all wips and finals and the first final post.
- - Tag whether they are uploaded to FA and e621 (Just the finals)
- - Add a meta tag tagging:needs_check
- - Choose rating as sfw or nsfw
- - Pools for multiple versions

How to check tagging:needs_check:
- Check tag categories
- Check for any URL files
- Check for comments & descriptions
- Check the rating