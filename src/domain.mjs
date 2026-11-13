export function topology(data) {
 return data.channels.map(channel=>({id:channel.id,capacity:channel.capacity,aliases:[...channel.aliases],proof:channel.proof}));
}
export function siteLimit(data,site){const record=data.sites.find(value=>value.id===site);if(!record)throw new Error('未知站点');return record.limit;}
export function preview(data){return topology(data);}
